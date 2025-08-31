# Rust Tasking

Simple tasking utility that automatically replaces tasks based on descriptors.

> Currently only [tokio] implementation.

## Utilities

- `Task`: update by descriptor, abort on drop.
- `TaskHub`: update by a list of descriptors, using `Task` internally.

## Usage

```rust
#[derive(PartialEq, Clone, Debug)]
struct TaskDescriptor {
  data: String,
}

let task = Task::new(
  "example",
  |TaskDescriptor { data }, abort_receiver| async move {
    println!("task data: {data}");

    abort_receiver.await?;

    Ok(())
  },
  Default::default(),
);

task
  .update(TaskDescriptor {
    data: "foo".to_owned(),
  })
  .await;
```

Checkout [examples](examples) for usage.

## License

MIT License.

[tokio]: https://tokio.rs/
