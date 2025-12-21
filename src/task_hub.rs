use std::{
  collections::{HashMap, HashSet},
  future::Future,
  hash::Hash,
  sync::{Arc, Mutex},
};

use crate::{AbortReceiver, Task, TaskDescriptor, TaskOptions, task_counter::TaskCounter};

pub struct TaskHub<TTaskKey, TTask, TTaskDescriptor> {
  name_prefix: String,
  task: Arc<Mutex<TTask>>,
  task_options: TaskOptions,
  task_map: Arc<Mutex<HashMap<TTaskKey, Arc<Task<TTask, TTaskDescriptor>>>>>,
  task_counter: Arc<TaskCounter>,
}

impl<
  TTaskKey: Clone + Eq + Hash + ToString + Send + 'static,
  TTask: FnMut(TTaskDescriptor, AbortReceiver) -> TTaskFuture + Send + 'static,
  TTaskReturn: Into<anyhow::Result<()>>,
  TTaskFuture: Future<Output = TTaskReturn> + Send + 'static,
  TTaskDescriptor: TaskDescriptor,
> TaskHub<TTaskKey, TTask, TTaskDescriptor>
{
  pub fn new(name_prefix: impl Into<String>, task: TTask, task_options: TaskOptions) -> Self {
    Self::new_with_counter(
      name_prefix,
      task,
      task_options,
      Arc::new(TaskCounter::new()),
    )
  }

  pub fn new_with_counter(
    name_prefix: impl Into<String>,
    task: TTask,
    task_options: TaskOptions,
    task_counter: Arc<TaskCounter>,
  ) -> Self {
    Self {
      name_prefix: name_prefix.into(),
      task: Arc::new(Mutex::new(task)),
      task_options,
      task_map: Arc::new(Mutex::new(HashMap::new())),
      task_counter,
    }
  }

  pub fn prune_inactive(&self) {
    self
      .task_map
      .lock()
      .unwrap()
      .retain(|_, task| task.is_active());
  }

  pub async fn update(&self, descriptors: impl IntoIterator<Item = (TTaskKey, TTaskDescriptor)>) {
    self.update_tasks(descriptors, false).await;
  }

  pub async fn merge(&self, descriptors: impl IntoIterator<Item = (TTaskKey, TTaskDescriptor)>) {
    self.update_tasks(descriptors, true).await;
  }

  pub async fn clear(&self) {
    self.task_map.lock().unwrap().clear();

    self.task_counter.cleared().await;
  }

  async fn update_tasks(
    &self,
    descriptors: impl IntoIterator<Item = (TTaskKey, TTaskDescriptor)>,
    keep_existing_tasks: bool,
  ) {
    let descriptors = descriptors.into_iter().collect::<Vec<_>>();

    let key_set = descriptors
      .iter()
      .map(|(key, _)| key)
      .cloned()
      .collect::<HashSet<_>>();

    if !keep_existing_tasks {
      self
        .task_map
        .lock()
        .unwrap()
        .retain(|key, _| key_set.contains(key));
    }

    let futures = descriptors
      .into_iter()
      .map(|(key, descriptor)| self.add_task(key, descriptor))
      .collect::<Vec<_>>();

    futures::future::join_all(futures).await;
  }

  async fn add_task(&self, key: TTaskKey, descriptor: TTaskDescriptor) {
    let task = self
      .task_map
      .lock()
      .unwrap()
      .entry(key.clone())
      .or_insert_with(|| {
        Arc::new(
          Task::new_internal(
            format!(
              "{prefix}:{key}",
              prefix = self.name_prefix,
              key = key.to_string()
            ),
            self.task.clone(),
            self.task_options.clone(),
          )
          .with_counter(self.task_counter.clone()),
        )
      })
      .clone();

    task.update(descriptor).await;
  }
}
