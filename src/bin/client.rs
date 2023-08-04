use tokio::{net::TcpStream, io::{AsyncWriteExt, AsyncReadExt}};
use bytes::BytesMut;
use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
struct Cli {
  #[clap(subcommand)]
  command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
  Get {
    key: String,
  },
  Set {
    key: String,
    value: String,
  }
}


#[tokio::main]
pub async fn main() -> Result<(), std::io::Error> {
  let args = Cli::parse();
  let mut stream = TcpStream::connect("127.0.0.1:8081").await?;

  match args.command {
    Command::Set { key, value } => {
      stream.write_all(b"set").await?;
      stream.write_all(b" ").await?;
      stream.write_all(&key.as_bytes()).await?;
      stream.write_all(b" ").await?;
      stream.write_all(&value.as_bytes()).await?;

      let mut buf = BytesMut::with_capacity(1024);
      let _length = stream.read_buf(&mut buf).await?;
      match std::str::from_utf8(&mut buf) {
        Ok(resp) => {
          if resp == "r Ok" {
            println!("Key updated");
          } else if resp == "Ok" {
            println!("Key set");
          }
        },
        Err(err) => {
          println!("error: {}", err);
        }
      }
      Ok(())
    }
    Command::Get { key } => {
      stream.write_all(b"get").await?;
      stream.write_all(b" ").await?;
      stream.write_all(&key.as_bytes()).await?;
      let mut buf: BytesMut = BytesMut::with_capacity(1024);
      let _length = stream.read_buf(&mut buf).await?;
      println!("buffer read");
      match std::str::from_utf8(&mut buf) {
        Ok(resp) => {
          if resp == "" {
            println!("No such key found");
          } else {
            println!("Value: {}", resp);
          }
        },
        Err(err) => {
          println!("error: {}", err);
        }
      }
      Ok(())
    }
  }
}
