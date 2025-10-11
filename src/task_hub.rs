use std::{
  collections::{HashMap, HashSet},
  fmt,
  future::Future,
  hash::Hash,
  sync::{Arc, Mutex},
};

use crate::{
  AbortReceiver, DefaultComparator, Task, TaskOptions, default_descriptor_comparator,
  task_counter::TaskCounter,
};

pub struct TaskHub<
  TTaskKey,
  TTask,
  TDescriptor,
  TDescriptorComparator = DefaultComparator<TDescriptor>,
> {
  name_prefix: String,
  task: Arc<Mutex<TTask>>,
  descriptor_comparator: Arc<TDescriptorComparator>,
  task_options: TaskOptions,
  task_map: Arc<Mutex<HashMap<TTaskKey, Arc<Task<TTask, TDescriptor, TDescriptorComparator>>>>>,
  task_counter: Arc<TaskCounter>,
}

impl<
  TTaskKey: Clone + Eq + Hash + ToString + Send + 'static,
  TTask: Fn(TDescriptor, AbortReceiver) -> TTaskFuture + Send + 'static,
  TTaskReturn: Into<anyhow::Result<()>>,
  TTaskFuture: Future<Output = TTaskReturn> + Send + 'static,
  TDescriptor: Clone + fmt::Debug + Send + 'static,
  TDescriptorComparator: (Fn(&TDescriptor, &TDescriptor) -> bool) + Send + Sync + 'static,
> TaskHub<TTaskKey, TTask, TDescriptor, TDescriptorComparator>
{
  pub fn new_with_comparator(
    name_prefix: impl Into<String>,
    task: TTask,
    descriptor_comparator: TDescriptorComparator,
    task_options: TaskOptions,
  ) -> Self {
    Self::new_with_comparator_and_counter(
      name_prefix,
      task,
      descriptor_comparator,
      task_options,
      Arc::new(TaskCounter::new()),
    )
  }

  pub fn new_with_comparator_and_counter(
    name_prefix: impl Into<String>,
    task: TTask,
    descriptor_comparator: TDescriptorComparator,
    task_options: TaskOptions,
    task_counter: Arc<TaskCounter>,
  ) -> Self {
    Self {
      name_prefix: name_prefix.into(),
      task: Arc::new(Mutex::new(task)),
      descriptor_comparator: Arc::new(descriptor_comparator),
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

  pub async fn update(&self, descriptors: Vec<(TTaskKey, TDescriptor)>) {
    self.update_tasks(descriptors, false).await;
  }

  pub async fn merge(&self, descriptors: Vec<(TTaskKey, TDescriptor)>) {
    self.update_tasks(descriptors, true).await;
  }

  pub async fn clear(&self) {
    self.task_map.lock().unwrap().clear();

    self.task_counter.cleared().await;
  }

  async fn update_tasks(
    &self,
    descriptors: Vec<(TTaskKey, TDescriptor)>,
    keep_existing_tasks: bool,
  ) {
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

  async fn add_task(&self, key: TTaskKey, descriptor: TDescriptor) {
    let task = self
      .task_map
      .lock()
      .unwrap()
      .entry(key.clone())
      .or_insert_with(|| {
        Arc::new(
          Task::new_with_comparator_internal(
            format!(
              "{prefix}:{key}",
              prefix = self.name_prefix,
              key = key.to_string()
            ),
            self.task.clone(),
            self.descriptor_comparator.clone(),
            self.task_options.clone(),
          )
          .with_counter(self.task_counter.clone()),
        )
      })
      .clone();

    task.update(descriptor).await;
  }
}

impl<
  TTaskKey: Clone + Eq + Hash + ToString + Send + 'static,
  TTask: Fn(TDescriptor, AbortReceiver) -> TTaskFuture + Send + 'static,
  TTaskReturn: Into<anyhow::Result<()>>,
  TTaskFuture: Future<Output = TTaskReturn> + Send + 'static,
  TDescriptor: PartialEq + Clone + fmt::Debug + Send + 'static,
> TaskHub<TTaskKey, TTask, TDescriptor, DefaultComparator<TDescriptor>>
{
  pub fn new(name_prefix: impl Into<String>, task: TTask, options: TaskOptions) -> Self {
    Self::new_with_comparator(name_prefix, task, default_descriptor_comparator, options)
  }

  pub fn new_with_counter(
    name_prefix: impl Into<String>,
    task: TTask,
    options: TaskOptions,
    task_counter: Arc<TaskCounter>,
  ) -> Self {
    Self::new_with_comparator_and_counter(
      name_prefix,
      task,
      default_descriptor_comparator,
      options,
      task_counter,
    )
  }
}
