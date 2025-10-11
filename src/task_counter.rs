use std::sync::Mutex;

pub struct TaskCounter {
  counter: Mutex<usize>,
  counter_sender: Mutex<tokio::sync::watch::Sender<usize>>,
  _counter_receiver: tokio::sync::watch::Receiver<usize>,
}

impl TaskCounter {
  pub fn new() -> Self {
    let (counter_sender, counter_receiver) = tokio::sync::watch::channel(0);

    Self {
      counter: Mutex::new(0),
      counter_sender: Mutex::new(counter_sender),
      _counter_receiver: counter_receiver,
    }
  }

  pub fn inc(&self) {
    *self.counter.lock().unwrap() += 1;
  }

  pub fn dec(&self) {
    let mut counter = self.counter.lock().unwrap();

    *counter -= 1;

    self.counter_sender.lock().unwrap().send(*counter).unwrap();
  }

  pub async fn cleared(&self) {
    let mut counter_receiver = self.counter_sender.lock().unwrap().subscribe();

    while *counter_receiver.borrow_and_update() > 0 {
      counter_receiver.changed().await.unwrap();
    }
  }
}

impl Default for TaskCounter {
  fn default() -> Self {
    Self::new()
  }
}
