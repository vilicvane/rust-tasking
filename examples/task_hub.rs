use lits::duration;
use tasking::TaskHub;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
  env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("tasking=")).init();

  #[derive(PartialEq, Clone, Debug)]
  struct TaskDescriptor {
    data: String,
  }

  let task_hub = TaskHub::new(
    "example",
    |TaskDescriptor { data }, abort_receiver| async move {
      log::info!("task data: {data}");

      abort_receiver.await?;

      Ok(())
    },
    Default::default(),
  );

  let foo_key = "foo".to_owned();
  let bar_key = "bar".to_owned();

  task_hub
    .update(vec![
      (
        foo_key.clone(),
        TaskDescriptor {
          data: "foo 1".to_owned(),
        },
      ),
      (
        bar_key.clone(),
        TaskDescriptor {
          data: "bar 1".to_owned(),
        },
      ),
    ])
    .await;

  tokio::time::sleep(duration!("1s")).await;

  task_hub
    .merge(vec![(
      foo_key.clone(),
      TaskDescriptor {
        data: "foo 2".to_owned(),
      },
    )])
    .await;

  tokio::time::sleep(duration!("1s")).await;

  task_hub
    .update(vec![(
      bar_key.clone(),
      TaskDescriptor {
        data: "bar 2".to_owned(),
      },
    )])
    .await;

  tokio::time::sleep(duration!("1s")).await;

  drop(task_hub);

  tokio::signal::ctrl_c().await?;

  Ok(())
}
