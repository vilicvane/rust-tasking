# Rust Tasking

Simple tasking utility that automatically replaces tasks based on descriptors.

> Currently only [tokio] implementation.

## Utilities

- `Task`: update by descriptor, abort on drop.
- `TaskHub`: update by a list of descriptors, using `Task` internally.

## Usage

```rust
#[derive(PartialEq, Clone, Debug)]
struct MyTaskDescriptor {
  data: String,
}

impl TaskDescriptor for MyTaskDescriptor {
  fn compare(&self, other: &Self) -> bool {
    self == other
  }
}

let task = Task::new(
  "example",
  |MyTaskDescriptor { data }, abort_receiver| async move {
    println!("task data: {data}");

    abort_receiver.await?;

    Ok(())
  },
  Default::default(),
);

task
  .update(MyTaskDescriptor {
    data: "foo".to_owned(),
  })
  .await;
```

Checkout [examples](examples) for usage.

## License

MIT License.

[tokio]: https://tokio.rs/
