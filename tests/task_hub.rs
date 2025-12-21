use std::sync::{
  Arc,
  atomic::{AtomicUsize, Ordering},
};

use lits::duration;

use tasking::{EmptyTaskDescriptor, TaskHub, TaskOptions};

#[tokio::test]
async fn task_hub_adds_and_removes_tasks_on_update() {
  let starts = Arc::new(AtomicUsize::new(0));
  let starts_c = starts.clone();

  let options = TaskOptions {
    restart_on_error: false,
    restart_interval: duration!("10ms"),
    abort_timeout: Some(duration!("100ms")),
  };

  let hub = TaskHub::new(
    "hub",
    move |_d: EmptyTaskDescriptor, _abort| {
      let starts = starts_c.clone();

      async move {
        starts.fetch_add(1, Ordering::SeqCst);

        // run until aborted
        std::future::pending::<()>().await;

        Ok(())
      }
    },
    options,
  );

  // add two tasks
  hub
    .update(vec![
      ("a".to_string(), EmptyTaskDescriptor),
      ("b".to_string(), EmptyTaskDescriptor),
    ])
    .await;
  tokio::time::sleep(duration!("20ms")).await;

  assert_eq!(starts.load(Ordering::SeqCst), 2);

  // update to only include "a" -> "b" should be removed
  hub
    .update(vec![("a".to_string(), EmptyTaskDescriptor)])
    .await;
  tokio::time::sleep(duration!("20ms")).await;

  // still one task started for "a", total starts remains 2
  assert_eq!(starts.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn task_hub_merge_keeps_running_tasks() {
  let starts = Arc::new(AtomicUsize::new(0));
  let starts_c = starts.clone();

  let options = TaskOptions {
    restart_on_error: false,
    restart_interval: duration!("10ms"),
    abort_timeout: Some(duration!("100ms")),
  };

  let hub = TaskHub::new(
    "hub-merge",
    move |_d: EmptyTaskDescriptor, _abort| {
      let starts = starts_c.clone();

      async move {
        starts.fetch_add(1, Ordering::SeqCst);

        // run until aborted
        std::future::pending::<()>().await;

        Ok(())
      }
    },
    options,
  );

  // add two tasks
  hub
    .update(vec![
      ("x".to_string(), EmptyTaskDescriptor),
      ("y".to_string(), EmptyTaskDescriptor),
    ])
    .await;
  tokio::time::sleep(duration!("20ms")).await;

  assert_eq!(starts.load(Ordering::SeqCst), 2);

  // merge with only x -> y should be kept running because merge preserves running tasks
  hub
    .merge(vec![("x".to_string(), EmptyTaskDescriptor)])
    .await;
  tokio::time::sleep(duration!("20ms")).await;

  // no new starts should be created
  assert_eq!(starts.load(Ordering::SeqCst), 2);
}
