use lits::duration;
use tasking::{Task, TaskDescriptor};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
  env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("task,tasking"))
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

      Ok(())
    },
    Default::default(),
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
