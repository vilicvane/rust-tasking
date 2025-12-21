use std::sync::{
  Arc,
  atomic::{AtomicUsize, Ordering},
};

use lits::duration;

use tasking::{Task, TaskDescriptor, TaskOptions};

#[derive(Clone, Debug)]
struct EmptyTaskDescriptor {}

impl TaskDescriptor for EmptyTaskDescriptor {
  fn compare(&self, _other: &Self) -> bool {
    true
  }
}

#[tokio::test]
async fn descriptor_update_does_not_restart_for_equal_descriptors() {
  #[derive(PartialEq, Clone, Debug)]
  struct Descriptor(&'static str);

  impl TaskDescriptor for Descriptor {
    fn compare(&self, other: &Self) -> bool {
      self == other
    }
  }

  let starts = Arc::new(AtomicUsize::new(0));

  let starts_clone = starts.clone();

  let task = Task::new(
    "descriptor-test",
    move |_d: Descriptor, abort_receiver| {
      let starts = starts_clone.clone();

      async move {
        starts.fetch_add(1, Ordering::SeqCst);

        // keep running until aborted
        let _ = abort_receiver.await;

        Ok(())
      }
    },
    TaskOptions::default(),
  );

  // start with descriptor "a"
  task.update(Descriptor("a")).await;
  tokio::time::sleep(duration!("20ms")).await;

  assert_eq!(starts.load(Ordering::SeqCst), 1);
  assert!(task.is_active());

  // update with the same descriptor -> should not restart
  task.update(Descriptor("a")).await;
  tokio::time::sleep(duration!("20ms")).await;

  assert_eq!(starts.load(Ordering::SeqCst), 1);

  // update with a different descriptor -> should restart
  task.update(Descriptor("b")).await;
  tokio::time::sleep(duration!("20ms")).await;

  assert_eq!(starts.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn restart_on_error_disabled_runs_once_and_stops() {
  let runs = Arc::new(AtomicUsize::new(0));
  let runs_clone = runs.clone();

  let options = TaskOptions {
    restart_on_error: false,
    restart_interval: duration!("10ms"),
    abort_timeout: Some(duration!("30s")),
  };

  let task = Task::new(
    "restart-disabled",
    move |_d: EmptyTaskDescriptor, _abort| {
      let runs = runs_clone.clone();

      async move {
        runs.fetch_add(1, Ordering::SeqCst);

        Err::<(), anyhow::Error>(anyhow::anyhow!("simulated error"))
      }
    },
    options,
  );

  task.update(EmptyTaskDescriptor {}).await;

  // allow the single run to complete
  tokio::time::sleep(duration!("50ms")).await;

  assert_eq!(runs.load(Ordering::SeqCst), 1);
  assert!(!task.is_active());
}

#[tokio::test]
async fn restart_on_error_enabled_restarts_after_failure() {
  let runs = Arc::new(AtomicUsize::new(0));
  let runs_clone = runs.clone();

  let options = TaskOptions {
    restart_on_error: true,
    restart_interval: duration!("10ms"),
    abort_timeout: Some(duration!("30s")),
  };

  let task = Task::new(
    "restart-enabled",
    move |_d: EmptyTaskDescriptor, _abort| {
      let runs = runs_clone.clone();

      async move {
        runs.fetch_add(1, Ordering::SeqCst);

        // always fail to trigger restart loop
        Err::<(), anyhow::Error>(anyhow::anyhow!("simulated error"))
      }
    },
    options,
  );

  task.update(EmptyTaskDescriptor {}).await;

  // allow time for a couple of restarts
  tokio::time::sleep(duration!("80ms")).await;

  // should have restarted at least once
  assert!(runs.load(Ordering::SeqCst) >= 2);
}

#[tokio::test]
async fn abort_graceful_on_drop() {
  let started = Arc::new(AtomicUsize::new(0));
  let aborted = Arc::new(AtomicUsize::new(0));

  let started_c = started.clone();
  let aborted_c = aborted.clone();

  let options = TaskOptions {
    restart_on_error: false,
    restart_interval: duration!("10ms"),
    abort_timeout: Some(duration!("200ms")),
  };

  let task = Task::new(
    "abort-graceful",
    move |_d: EmptyTaskDescriptor, abort_receiver| {
      let started = started_c.clone();
      let aborted = aborted_c.clone();

      async move {
        started.fetch_add(1, Ordering::SeqCst);

        // wait for the abort signal and then exit gracefully
        let abort = abort_receiver.await?;

        assert!(!abort.replaced());

        aborted.fetch_add(1, Ordering::SeqCst);

        Ok(())
      }
    },
    options,
  );

  task.update(EmptyTaskDescriptor {}).await;

  tokio::time::sleep(duration!("20ms")).await;

  assert_eq!(started.load(Ordering::SeqCst), 1);

  drop(task);

  // allow time for graceful abort to complete
  tokio::time::sleep(duration!("100ms")).await;

  assert_eq!(aborted.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn abort_forced_when_task_ignores_abort() {
  let counter = Arc::new(AtomicUsize::new(0));
  let counter_c = counter.clone();

  let options = TaskOptions {
    restart_on_error: false,
    restart_interval: duration!("10ms"),
    // short timeout so test runs quickly
    abort_timeout: Some(duration!("30ms")),
  };

  let task = Task::new(
    "abort-forced",
    move |_d: EmptyTaskDescriptor, _abort| {
      let counter = counter_c.clone();

      async move {
        // initial increment
        counter.fetch_add(1, Ordering::SeqCst);

        // ignore abort receiver and keep doing work
        loop {
          tokio::time::sleep(duration!("10ms")).await;
          counter.fetch_add(1, Ordering::SeqCst);
        }

        #[allow(unreachable_code)]
        Ok(())
      }
    },
    options,
  );

  task.update(EmptyTaskDescriptor {}).await;

  // let the task run a bit
  tokio::time::sleep(duration!("60ms")).await;

  let before = counter.load(Ordering::SeqCst);
  assert!(before >= 2, "expected a couple of increments before drop");

  drop(task);

  // allow time for abort_timeout to elapse and force abort to happen
  tokio::time::sleep(duration!("120ms")).await;

  let after = counter.load(Ordering::SeqCst);

  // counter should stop increasing after forced abort
  assert_eq!(after, before, "counter should stop after forced abort");
}
