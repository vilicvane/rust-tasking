use std::{
  fmt,
  future::Future,
  pin::Pin,
  sync::{Arc, Mutex},
  time::Duration,
};

use lits::duration;

use crate::task_counter::TaskCounter;

pub struct Task<TTask, TTaskDescriptor> {
  pub(crate) name: String,
  task: Arc<Mutex<TTask>>,
  options: TaskOptions,
  descriptor: Mutex<Option<TTaskDescriptor>>,
  handle: Mutex<Option<tokio::task::JoinHandle<()>>>,
  instance: Arc<tokio::sync::Mutex<Option<Arc<TaskInstance>>>>,
  // Separated state from `instance` to avoid contagious async Mutex.
  instance_state: Mutex<Option<Arc<Mutex<TaskInstanceState>>>>,
  task_counter: Option<Arc<TaskCounter>>,
}

enum TaskInstanceState {
  Active,
  Completed,
  Aborted,
  Error,
}

type AbortSender = tokio::sync::oneshot::Sender<bool>;

pub struct AbortReceiver(tokio::sync::oneshot::Receiver<bool>);

impl Future for AbortReceiver {
  type Output = Result<Abort, tokio::sync::oneshot::error::RecvError>;

  fn poll(self: Pin<&mut Self>, context: &mut std::task::Context) -> std::task::Poll<Self::Output> {
    Pin::new(&mut self.get_mut().0)
      .poll(context)
      .map_ok(|replaced| Abort { replaced })
  }
}

#[derive(Clone, Debug, Default)]
pub struct Abort {
  replaced: bool,
}

impl Abort {
  pub fn replaced(&self) -> bool {
    self.replaced
  }
}

#[derive(Clone)]
pub struct TaskOptions {
  pub restart_on_error: bool,
  pub restart_interval: Duration,
  pub abort_timeout: Option<Duration>,
}

impl Default for TaskOptions {
  fn default() -> Self {
    Self {
      restart_on_error: true,
      restart_interval: duration!("1s"),
      abort_timeout: Some(duration!("30s")),
    }
  }
}

struct TaskInstance {
  being_aborted: Mutex<bool>,
  abort_sender: Mutex<Option<AbortSender>>,
}

impl<TTask, TTaskReturn, TTaskFuture, TTaskDescriptor> Task<TTask, TTaskDescriptor>
where
  TTask: FnMut(TTaskDescriptor, AbortReceiver) -> TTaskFuture + Send + 'static,
  TTaskReturn: Into<anyhow::Result<()>>,
  TTaskFuture: Future<Output = TTaskReturn> + Send + 'static,
  TTaskDescriptor: TaskDescriptor,
{
  pub fn new(name: impl Into<String>, task: TTask, options: TaskOptions) -> Self {
    Self::new_internal(name.into(), Arc::new(Mutex::new(task)), options)
  }

  pub(crate) fn new_internal(name: String, task: Arc<Mutex<TTask>>, options: TaskOptions) -> Self {
    Self {
      name,
      task,
      options,
      descriptor: Mutex::new(None),
      handle: Mutex::new(None),
      instance: Arc::new(tokio::sync::Mutex::new(None)),
      instance_state: Mutex::new(None),
      task_counter: None,
    }
  }

  pub fn with_counter(mut self, task_counter: Arc<TaskCounter>) -> Self {
    task_counter.inc();

    self.task_counter = Some(task_counter);
    self
  }

  pub fn is_active(&self) -> bool {
    if let Some(state) = self.instance_state.lock().unwrap().as_ref() {
      matches!(*state.lock().unwrap(), TaskInstanceState::Active)
    } else {
      false
    }
  }

  pub async fn update(&self, new_descriptor: TTaskDescriptor) {
    let mut instance = self.instance.lock().await;

    if let Some(descriptor) = self.descriptor.lock().unwrap().as_ref()
      && descriptor.compare(&new_descriptor)
    {
      return;
    }

    log::info!("[{name}] update: {new_descriptor:?}", name = self.name);

    let state = Arc::new(Mutex::new(TaskInstanceState::Active));

    *self.instance_state.lock().unwrap() = Some(state.clone());

    if let Some(instance) = instance.as_ref() {
      let handle = self.handle.lock().unwrap().take().unwrap();

      if !handle.is_finished() {
        *instance.being_aborted.lock().unwrap() = true;

        let abort_sender = instance.abort_sender.lock().unwrap().take();

        abort(
          self.name.clone(),
          handle,
          abort_sender,
          self.options.abort_timeout,
          true,
        )
        .await;
      }
    }

    let new_instance = Arc::new(TaskInstance {
      being_aborted: Mutex::new(false),
      abort_sender: Mutex::new(None),
    });

    let instance_future = {
      let name = self.name.clone();
      let task = self.task.clone();
      let options = self.options.clone();

      let instance = new_instance.clone();
      let descriptor = new_descriptor.clone();

      let task_counter = self.task_counter.clone();

      async move {
        if let Some(task_counter) = task_counter.as_ref() {
          task_counter.inc();
        }

        loop {
          let (abort_sender, abort_receiver) = tokio::sync::oneshot::channel();

          instance.abort_sender.lock().unwrap().replace(abort_sender);

          let task_future =
            (*task.lock().unwrap())(descriptor.clone(), AbortReceiver(abort_receiver));

          log::info!("[{name}] task instance started.");

          let result = task_future.await.into();

          let being_aborted = *instance.being_aborted.lock().unwrap();

          match result {
            Ok(()) => {
              if being_aborted {
                log::info!("[{name}] task instance aborted gracefully.");

                *state.lock().unwrap() = TaskInstanceState::Aborted;
              } else {
                log::info!("[{name}] task instance completed.");

                *state.lock().unwrap() = TaskInstanceState::Completed;
              }

              break;
            }
            Err(error) => {
              log::error!("[{name}] task instance error: {error:?}");

              if options.restart_on_error {
                tokio::time::sleep(options.restart_interval).await;
              } else {
                *state.lock().unwrap() = TaskInstanceState::Error;
                break;
              }
            }
          }
        }

        if let Some(task_counter) = task_counter.as_ref() {
          task_counter.dec();
        }
      }
    };

    let handle = tokio::spawn(instance_future);

    self.handle.lock().unwrap().replace(handle);
    self.descriptor.lock().unwrap().replace(new_descriptor);

    instance.replace(new_instance);
  }
}

impl<TTask, TTaskDescriptor> Drop for Task<TTask, TTaskDescriptor> {
  fn drop(&mut self) {
    let task_name = self.name.clone();

    if let Some(task_counter) = self.task_counter.as_ref() {
      task_counter.dec();
    }

    log::info!("[{task_name}] task dropped.");

    tokio::spawn({
      let Some(handle) = self.handle.lock().unwrap().take() else {
        return;
      };

      let instance = self.instance.clone();
      let abort_timeout = self.options.abort_timeout;

      async move {
        let Some(instance) = instance.lock().await.take() else {
          return;
        };

        *instance.being_aborted.lock().unwrap() = true;

        let abort_sender = instance.abort_sender.lock().unwrap().take();

        abort(task_name, handle, abort_sender, abort_timeout, false).await;
      }
    });
  }
}

async fn abort(
  task_name: String,
  handle: tokio::task::JoinHandle<()>,
  abort_sender: Option<AbortSender>,
  abort_timeout: Option<Duration>,
  replaced: bool,
) {
  let should_wait = abort_sender.is_some_and(|abort_sender| abort_sender.send(replaced).is_ok());

  let result = if should_wait {
    if let Some(abort_timeout) = abort_timeout {
      let abort_handle = handle.abort_handle();

      match tokio::time::timeout(abort_timeout, handle).await {
        Ok(result) => result,
        Err(_) => {
          abort_handle.abort();

          log::info!("[{task_name}] task instance aborted after graceful attempt timed out.",);

          Ok(())
        }
      }
    } else {
      handle.await
    }
  } else {
    handle.abort();

    log::info!("[{task_name}] task instance aborted.");

    Ok(())
  };

  if let Err(error) = result {
    log::error!("[{task_name}] task instance aborted with error: {error:?}");
  }
}

pub trait TaskDescriptor
where
  Self: Clone + fmt::Debug + Send + 'static,
{
  fn compare(&self, other: &Self) -> bool;
}
