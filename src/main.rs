use std::io;
use tokio::net::{TcpListener, TcpStream};
use tokio::stream::StreamExt;

mod redis;

#[tokio::main]
async fn main() -> io::Result<()> {
    let mut listener = TcpListener::bind("127.0.0.1:6379").await?;

    let mut incoming = listener.incoming();

    while let Some(stream) = incoming.next().await {
        match stream {
            Ok(stream) => {
                tokio::spawn(handle_connection(stream));
            }
            Err(e) => { eprintln!("{:?}", e); }
        }
    }
    Ok(())
}

async fn handle_connection(stream: TcpStream) -> io::Result<()> {
    let stream = tokio::io::BufStream::new(stream);
    let mut server = redis::Server::new(stream);
    loop {
        server.process_message().await?;
    }
}