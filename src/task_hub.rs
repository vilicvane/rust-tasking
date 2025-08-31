use std::{
  collections::{HashMap, HashSet},
  fmt,
  future::Future,
  hash::Hash,
  sync::{Arc, Mutex},
};

use crate::{AbortReceiver, DefaultComparator, Task, TaskOptions, default_descriptor_comparator};

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
    Self {
      name_prefix: name_prefix.into(),
      task: Arc::new(Mutex::new(task)),
      descriptor_comparator: Arc::new(descriptor_comparator),
      task_options,
      task_map: Arc::new(Mutex::new(HashMap::new())),
    }
  }

  pub async fn update(&self, descriptors: Vec<(TTaskKey, TDescriptor)>) {
    self.update_tasks(descriptors, false).await;
  }

  pub async fn merge(&self, descriptors: Vec<(TTaskKey, TDescriptor)>) {
    self.update_tasks(descriptors, true).await;
  }

  async fn update_tasks(
    &self,
    descriptors: Vec<(TTaskKey, TDescriptor)>,
    keep_running_tasks: bool,
  ) {
    let mut pending_task_key_set = self
      .task_map
      .lock()
      .unwrap()
      .keys()
      .cloned()
      .collect::<HashSet<_>>();

    for (key, descriptor) in descriptors {
      pending_task_key_set.remove(&key);

      self.add_task(key.clone(), descriptor).await;
    }

    let mut task_map = self.task_map.lock().unwrap();

    if keep_running_tasks {
      for key in pending_task_key_set {
        let task = task_map.get(&key);

        if let Some(task) = task {
          if task.is_running() {
            continue;
          }

          task_map.remove(&key);
        }
      }
    } else {
      for key in pending_task_key_set {
        task_map.remove(&key);
      }
    }
  }

  async fn add_task(&self, key: TTaskKey, descriptor: TDescriptor) {
    let task = self
      .task_map
      .lock()
      .unwrap()
      .entry(key.clone())
      .or_insert_with(|| {
        Arc::new(Task::new_with_comparator_internal(
          format!(
            "{prefix}:{key}",
            prefix = self.name_prefix,
            key = key.to_string()
          ),
          self.task.clone(),
          self.descriptor_comparator.clone(),
          self.task_options.clone(),
        ))
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
}
