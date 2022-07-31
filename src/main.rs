use std::io;
use tokio::net::TcpListener;
use tokio::stream::StreamExt;

mod redis;

#[tokio::main]
async fn main() -> io::Result<()> {
    let mut listener = TcpListener::bind("127.0.0.1:6379").await?;

    let mut incoming = listener.incoming();
    let server = redis::Server::new();

    while let Some(stream) = incoming.next().await {
        match stream {
            Ok(stream) => {
                let stream = tokio::io::BufStream::new(stream);
                let worker = server.worker(stream);
                tokio::spawn(worker.run());
            }
            Err(e) => {
                eprintln!("{:?}", e);
            }
        }
    }
    Ok(())
}
