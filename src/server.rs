use crate::Handler;
use crate::Listener;

pub async fn run(listener: &Listener) -> std::io::Result<()> {
  loop {
    let socket = listener.accept().await?;
    let mut handler = Handler::new(listener, socket);

    tokio::spawn(async move {
      if let Err(_err) = process_method(&mut handler).await {
        println!("Connection error");
      }
    });
  }
}

async fn process_method(handler: &mut Handler) -> Result<(), std::io::Error> {
  while !handler.shutdown.is_shutdown() {
    let result = tokio::select! {
      _ = handler.shutdown.listen_recv() => {
        return Ok(());
      },
      res = handler.connection.read_buf_data() => res,
    };

    let (command, attrs) = match result {
      Some((command, attrs)) => (command, attrs),
      None => return Ok(()),
    };
    handler.process_query(command, attrs).await?;
  }

  Ok(())
}