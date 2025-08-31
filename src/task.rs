use std::{
  fmt,
  future::Future,
  sync::{Arc, Mutex},
  time::Duration,
};

use lits::duration;

pub type DefaultComparator<T> = fn(&T, &T) -> bool;

pub struct Task<TTask, TDescriptor, TDescriptorComparator> {
  pub(crate) name: String,
  task: Arc<Mutex<TTask>>,
  descriptor_comparator: Arc<TDescriptorComparator>,
  options: TaskOptions,
  instance: Mutex<Option<TaskInstance<TDescriptor>>>,
  abort: Arc<Mutex<Abort>>,
}

struct Abort {
  aborted: bool,
  sender: Option<AbortSender>,
}

pub type AbortSender = tokio::sync::oneshot::Sender<()>;
pub type AbortReceiver = tokio::sync::oneshot::Receiver<()>;

struct TaskInstance<TDescriptor> {
  descriptor: TDescriptor,
  handle: tokio::task::JoinHandle<()>,
}

#[derive(Clone)]
pub struct TaskOptions {
  pub restart_on_error: bool,
  pub restart_interval: Duration,
}

impl Default for TaskOptions {
  fn default() -> Self {
    Self {
      restart_on_error: true,
      restart_interval: duration!("1s"),
    }
  }
}

impl<
  TTask: Fn(TDescriptor, AbortReceiver) -> TTaskFuture + Send + 'static,
  TTaskReturn: Into<anyhow::Result<()>>,
  TTaskFuture: Future<Output = TTaskReturn> + Send + 'static,
  TDescriptor: Clone + fmt::Debug + Send + 'static,
  TDescriptorComparator: Fn(&TDescriptor, &TDescriptor) -> bool,
> Task<TTask, TDescriptor, TDescriptorComparator>
{
  pub fn new_with_comparator(
    name: impl Into<String>,
    task: TTask,
    descriptor_comparator: TDescriptorComparator,
    options: Option<TaskOptions>,
  ) -> Self {
    Self::new_with_comparator_internal(
      name,
      Arc::new(Mutex::new(task)),
      Arc::new(descriptor_comparator),
      options,
    )
  }

  pub(crate) fn new_with_comparator_internal(
    name: impl Into<String>,
    task: Arc<Mutex<TTask>>,
    descriptor_comparator: Arc<TDescriptorComparator>,
    options: Option<TaskOptions>,
  ) -> Self {
    Self {
      name: name.into(),
      task,
      descriptor_comparator,
      options: options.unwrap_or_default(),
      instance: Mutex::new(None),
      abort: Arc::new(Mutex::new(Abort {
        aborted: false,
        sender: None,
      })),
    }
  }

  pub async fn update(&self, new_descriptor: TDescriptor) {
    let instance = {
      let mut instance = self.instance.lock().unwrap();

      if let Some(instance) = instance.as_ref()
        && (self.descriptor_comparator)(&instance.descriptor, &new_descriptor)
      {
        return;
      }

      instance.take()
    };

    if let Some(instance) = instance {
      let should_wait = {
        let mut abort = self.abort.lock().unwrap();

        abort.aborted = true;

        abort
          .sender
          .take()
          .is_some_and(|abort_sender| abort_sender.send(()).is_ok())
      };

      if should_wait {
        instance.handle.await.ok();
      } else {
        log::info!("[{name}] task aborted.", name = self.name);

        instance.handle.abort();
      }
    }

    log::info!("[{name}] update: {new_descriptor:?}", name = self.name);

    let new_handle = tokio::spawn({
      let name = self.name.clone();
      let task = self.task.clone();
      let new_descriptor = new_descriptor.clone();
      let options = self.options.clone();

      let abort = self.abort.clone();

      async move {
        loop {
          let (abort_sender, abort_receiver) = tokio::sync::oneshot::channel();

          abort.lock().unwrap().sender.replace(abort_sender);

          let task_future = (*task.lock().unwrap())(new_descriptor.clone(), abort_receiver);

          log::info!("[{name}] task started.");

          let result = task_future.await.into();

          let aborted = abort.lock().unwrap().aborted;

          match result {
            Ok(()) => {
              if aborted {
                log::info!("[{name}] task aborted gracefully.");
              } else {
                log::info!("[{name}] task completed.");
              }

              break;
            }
            Err(error) => {
              log::error!("[{name}] task error: {error:?}");

              if !options.restart_on_error {
                break;
              }
            }
          }

          if aborted {
            break;
          }

          tokio::time::sleep(options.restart_interval).await;
        }
      }
    });

    self.instance.lock().unwrap().replace(TaskInstance {
      descriptor: new_descriptor,
      handle: new_handle,
    });
  }

  pub fn is_running(&self) -> bool {
    self
      .instance
      .lock()
      .unwrap()
      .as_ref()
      .is_some_and(|instance| !instance.handle.is_finished())
  }
}

impl<TTask, TDescriptor, TDescriptorComparator> Drop
  for Task<TTask, TDescriptor, TDescriptorComparator>
{
  fn drop(&mut self) {
    if let Some(instance) = self.instance.lock().unwrap().take() {
      let mut abort = self.abort.lock().unwrap();

      abort.aborted = true;

      if abort
        .sender
        .take()
        .is_none_or(|abort_sender| abort_sender.send(()).is_err())
      {
        instance.handle.abort();

        log::info!("[{name}] task dropped.", name = self.name);
      } else {
        let name = self.name.clone();

        tokio::spawn(async move {
          instance.handle.await.ok();

          log::info!("[{name}] task dropped.");
        });
      }
    }
  }
}

impl<
  TTask: Fn(TDescriptor, AbortReceiver) -> TTaskFuture + Send + 'static,
  TTaskReturn: Into<anyhow::Result<()>>,
  TTaskFuture: Future<Output = TTaskReturn> + Send + 'static,
  TDescriptor: PartialEq + Clone + fmt::Debug + Send + 'static,
> Task<TTask, TDescriptor, DefaultComparator<TDescriptor>>
{
  pub fn new(name: impl Into<String>, task: TTask, options: Option<TaskOptions>) -> Self {
    Self::new_with_comparator(name, task, default_descriptor_comparator, options)
  }
}

pub fn default_descriptor_comparator<TDescriptor: PartialEq>(
  a: &TDescriptor,
  b: &TDescriptor,
) -> bool {
  a == b
}
