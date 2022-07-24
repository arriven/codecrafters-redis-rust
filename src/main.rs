use std::io;
use tokio::io::{AsyncReadExt, AsyncWriteExt, AsyncBufReadExt};
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

async fn handle_connection(stream: TcpStream) -> io::Result<()> {
    let stream = tokio::io::BufStream::new(stream);
    let mut processor = Processor::new(stream);
    while let Ok(()) = processor.process_message().await {
        // let response = "+PONG\r\n";

        // stream.write(response.as_bytes()).await.unwrap();
        // stream.flush().await.unwrap();
    }
    Ok(())
}

enum Value {
    Nil,
    Int(i64),
    String(String),
    Array(usize, Vec<Value>)
}

impl Value {
    fn is_complete(&self) -> bool {
        match &self {
            Value::Array(size, data) => if *size == data.len() {
                data.iter().all(|val| val.is_complete())
            } else {
                false
            }
            _ => true
        }
    }

    fn append(&mut self, val: Value) {
        assert!(!self.is_complete());

        if let Value::Array(size, ref mut data) = self {
            if let Some(ref mut child) = data.iter_mut().find(|x| !x.is_complete()) {
                child.append(val);
            } else {
                assert!(data.len() < *size);
                data.push(val);
            }
        } else {
            assert!(false); // if should always pass
        }
    }

    fn to_command(self) -> Command {
        match self {
            Value::Array(_, mut data) => {
                assert!(data.len() > 0);
                match &data[0] {
                    Value::String(command) => match command.to_lowercase().as_str() {
                        "ping" => Command::Ping,
                        "echo" => {
                            if let Value::String(data) = data.pop().unwrap() {
                                Command::Echo(data)
                            } else {
                                Command::Error("ECHO: wrong argument type".to_owned())
                            }
                        },
                        "get" => {
                            if let Value::String(name) = data.pop().unwrap() {
                                Command::Get(name)
                            } else {
                                Command::Error("ECHO: wrong argument type".to_owned())
                            }
                        }
                        "set" => {
                            if data.len() != 3 {
                                Command::Error(format!{"wrong number of arguments for set: {}", data.len()})
                            } else {
                                let value = data.pop().unwrap();
                                if let Value::String(name) = data.pop().unwrap() {
                                    Command::Set(name, value)
                                } else {
                                    Command::Error("ECHO: wrong argument type".to_owned())
                                }
                            }
                        },
                        _ => Command::Error(format!("not implemented: {}", command)),
                    },
                    _ => Command::Error("wrong argument type".to_owned()),
                }
            },
            Value::String(data) => {
                match data.to_lowercase().as_str() {
                    "ping" => Command::Ping,
                    _ => Command::Error(format!("not implemented: {}", data)),
                }
            },
            _ => Command::Error("wrong argument type".to_owned()),
        }
    }
}

enum Command {
    Error(String),
    Ping,
    Echo(String),
    Get(String),
    Set(String, Value),
}

struct Processor<R> where R: tokio::prelude::AsyncRead + tokio::prelude::AsyncBufRead + tokio::prelude::AsyncWrite + std::marker::Unpin {
    stream: R,
    storage: std::collections::HashMap<String, Value>,
}

impl<R> Processor<R> where R: tokio::prelude::AsyncRead + tokio::prelude::AsyncBufRead + tokio::prelude::AsyncWrite + std::marker::Unpin {
    fn new(stream: R) -> Processor<R> {
        Processor{
            stream: stream,
            storage: std::collections::HashMap::new(),
        }
    }

    async fn process_message(&mut self)-> io::Result<()> {
        let message = self.read_message().await?;
        let command = message.to_command();
        match command {
            Command::Ping => {
                let response = "+PONG\r\n";

                self.stream.write(response.as_bytes()).await?;
                self.stream.flush().await?;
            }
            Command::Echo(data) => {
                let response = format!("+{}\r\n", data);

                self.stream.write(response.as_bytes()).await?;
                self.stream.flush().await?;
            }
            Command::Get(name) => {
                if let Some(Value::String(value)) = self.storage.get(&name) {
                    let response = format!("+{}\r\n", value);

                    self.stream.write(response.as_bytes()).await?;
                    self.stream.flush().await?;
                } else {
                    let response = "$-1\r\n";
    
                    self.stream.write(response.as_bytes()).await?;
                    self.stream.flush().await?;
                }
            }
            Command::Set(name, value) => {
                self.storage.insert(name, value);
                let response = "+OK\r\n";

                self.stream.write(response.as_bytes()).await?;
                self.stream.flush().await?;
            }
            Command::Error(cause) => {
                eprintln!("{}", cause);
            }
        }
        Ok(())
    }

    async fn read_message(&mut self) -> io::Result<Value> {
        let mut message = self.read_single().await?;
        while !message.is_complete() {
            let next = self.read_single().await?;
            message.append(next);
        }
        Ok(message)
    }

    async fn read_single(&mut self) -> io::Result<Value> {
        let b = self.stream.read_u8().await?;
        eprintln!("{}", b as char);
        match b as char {
            '*' => {
                let mut buf = vec![];
                self.stream.read_until('\n' as u8, &mut buf).await?;
                let text = buf.iter().map(|b| *b as char).collect::<String>();
                eprintln!("read size {:?}", buf);
                let size = text.trim().parse::<usize>().unwrap();
                Ok(Value::Array(size, Vec::with_capacity(size)))
            }
            '$' => {
                let mut buf = vec![];
                self.stream.read_until('\n' as u8, &mut buf).await?;
                let text = buf.iter().map(|b| *b as char).collect::<String>();
                eprintln!("read size {:?}", buf);
                let size = text.trim().parse::<i64>().unwrap();
                if size > 0 {
                    let size = size as usize;
                    let mut result = vec![0; size];
                    self.stream.read_exact(&mut result).await?;
                    let result = result.iter().map(|b| *b as char).collect::<String>();
                    eprintln!("read string {}", &result);
                    self.stream.read_until('\n' as u8, &mut buf).await?;
                    Ok(Value::String(result))
                } else {
                    Ok(Value::Nil)
                }
            }
            ':' => {
                let mut buf = vec![];
                self.stream.read_until('\n' as u8, &mut buf).await?;
                let result = buf.iter().map(|b| *b as char).collect::<String>().trim().parse::<i64>().unwrap();
                Ok(Value::Int(result))
            }
            _ => Ok(Value::Nil)
        }
    }
}

