use lits::duration;
use tasking::{Task, TaskDescriptor, TaskOptions};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
  env_logger::Builder::from_env(
    env_logger::Env::default().default_filter_or("task_abort_timeout,tasking"),
  )
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

  let task = Task::new(
    "example",
    |TestTaskDescriptor { data }, abort_receiver| async move {
      log::info!("task data: {data}");

      abort_receiver.await?;

      tokio::time::sleep(duration!("5s")).await;

      Ok(())
    },
    TaskOptions {
      abort_timeout: Some(duration!("1s")),
      ..Default::default()
    },
  );

  task
    .update(TestTaskDescriptor {
      data: "foo".to_owned(),
    })
    .await;

  tokio::time::sleep(duration!("1s")).await;

  task
    .update(TestTaskDescriptor {
      data: "bar".to_owned(),
    })
    .await;

  tokio::time::sleep(duration!("1s")).await;

  drop(task);

  tokio::signal::ctrl_c().await?;

  Ok(())
}
