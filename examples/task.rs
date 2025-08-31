use lits::duration;
use tasking::Task;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
  env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("tasking=")).init();

  #[derive(PartialEq, Clone, Debug)]
  struct TaskDescriptor {
    data: String,
  }

  let task = Task::new(
    "example",
    |TaskDescriptor { data }, abort_receiver| async move {
      log::info!("task data: {data}");

      abort_receiver.await?;

      Ok(())
    },
    None,
  );

  task
    .update(TaskDescriptor {
      data: "foo".to_owned(),
    })
    .await;

  tokio::time::sleep(duration!("1s")).await;

  task
    .update(TaskDescriptor {
      data: "bar".to_owned(),
    })
    .await;

  tokio::time::sleep(duration!("1s")).await;

  drop(task);

  tokio::signal::ctrl_c().await?;

  Ok(())
}
