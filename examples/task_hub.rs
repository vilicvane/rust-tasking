use lits::duration;
use tasking::{TaskDescriptor, TaskHub};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
  env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("task_hub,tasking"))
    .init();

  #[derive(PartialEq, Clone, Debug)]
  struct TestTaskDescriptor {
    data: String,
  }

  impl TaskDescriptor for TestTaskDescriptor {
    fn compare(&self, other: &Self) -> bool {
      self == other
    }
  }

  let task_hub = TaskHub::new(
    "example",
    |TestTaskDescriptor { data }, abort_receiver| async move {
      log::info!("task data: {data}");

      abort_receiver.await?;

      Ok(())
    },
    Default::default(),
  );

  let foo_key = "foo".to_owned();
  let bar_key = "bar".to_owned();

  log::info!("update [foo 1, bar 1]");

  task_hub
    .update(vec![
      (
        foo_key.clone(),
        TestTaskDescriptor {
          data: "foo 1".to_owned(),
        },
      ),
      (
        bar_key.clone(),
        TestTaskDescriptor {
          data: "bar 1".to_owned(),
        },
      ),
    ])
    .await;

  tokio::time::sleep(duration!("1s")).await;

  log::info!("merge [foo 2]");

  task_hub
    .merge(vec![(
      foo_key.clone(),
      TestTaskDescriptor {
        data: "foo 2".to_owned(),
      },
    )])
    .await;

  tokio::time::sleep(duration!("1s")).await;

  log::info!("update [bar 2]");

  task_hub
    .update(vec![(
      bar_key.clone(),
      TestTaskDescriptor {
        data: "bar 2".to_owned(),
      },
    )])
    .await;

  tokio::time::sleep(duration!("1s")).await;

  log::info!("clear task hub");

  task_hub.clear().await;

  log::info!("drop task hub");

  drop(task_hub);

  Ok(())
}
