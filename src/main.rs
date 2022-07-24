use std::io;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::stream::StreamExt;

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

async fn handle_connection(mut stream: TcpStream) {
    let mut buffer = [0; 1024];

    while let Ok(_) = stream.read(&mut buffer).await {
        let response = "+PONG\r\n";

        stream.write(response.as_bytes()).await.unwrap();
        stream.flush().await.unwrap();
    }
}

