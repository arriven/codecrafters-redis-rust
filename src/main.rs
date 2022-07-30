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
    loop {
        processor.process_message().await?;
    }
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
            Value::Array(size, data) => *size == data.len() && data.iter().all(Value::is_complete),
            _ => true,
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
            panic!("only array types can be appended with a value");
        }
    }

    fn into_command(self) -> Command {
        match self {
            Value::Array(_, data) => Command::from_array(data),
            Value::String(data) => Command::from_string(&data),
            _ => Command::Error("wrong argument type".to_owned()),
        }
    }
}

enum Command {
    Error(String),
    Ping,
    Echo(String),
    Get(String),
    Set(String, Value, Option<std::time::Instant>),
}

impl Command {
    fn from_string(data: &str) -> Command {
        match data.to_lowercase().as_str() {
            "ping" => Command::Ping,
            _ => Command::Error(format!("not implemented: {}", data)),
        }
    }

    fn from_array(data: Vec<Value>) -> Command {
        if data.is_empty() {
            return Command::Error("empty command".to_owned());
        }

        match &data[0] {
            Value::String(command) => match command.to_lowercase().as_str() {
                "ping" => Command::Ping,
                "echo" => Command::echo(data),
                "get" => Command::get(data),
                "set" => Command::set(data, None),
                _ => Command::Error(format!("not implemented: {}", command)),
            },
            _ => Command::Error("wrong argument type".to_owned()),
        }
    }

    fn echo(mut data: Vec<Value>) -> Command {
        if let Some(Value::String(data)) = data.pop() {
            Command::Echo(data)
        } else {
            Command::Error("ECHO: wrong argument type".to_owned())
        }
    }

    fn get(mut data: Vec<Value>) -> Command {
        if let Some(Value::String(data)) = data.pop() {
            Command::Get(data)
        } else {
            Command::Error("GET: wrong argument type".to_owned())
        }
    }

    fn set(mut data: Vec<Value>, expiry: Option<std::time::Instant>) -> Command {
        if data.len() < 3 {
            return Command::Error(format!{"not enough arguments for set: {}", data.len()})
        }
        if data.len() > 3 {
            return Command::set_with_flags(data)
        }

        let value = data.pop().unwrap();
        if let Value::String(name) = data.pop().unwrap() {
            Command::Set(name, value, expiry)
        } else {
            Command::Error("SET: wrong argument type".to_owned())
        }
    }

    fn set_with_flags(mut data: Vec<Value>) -> Command {
        let arg = data.pop().unwrap();
        if let Value::String(flag) = data.pop().unwrap() {
            match flag.to_lowercase().as_str() {
                "px" => {
                    let duration = if let Value::Int(duration) = arg {
                        duration as u64
                    } else if let Value::String(duration) = arg {
                        duration.parse::<u64>().unwrap()
                    } else {
                        return Command::Error("SET: wrong argument type".to_owned());
                    };
                    let expiry = std::time::Instant::now() + std::time::Duration::from_millis(duration as u64);
                    Command::set(data, Some(expiry))
                },
                _ => Command::Error(format!("SET: flag not implemented: {}", flag))
            }
        } else {
            Command::Error("SET: wrong argument type".to_owned())
        }
    }
}

struct StoredValue {
    value: Value,
    expiry: Option<std::time::Instant>,
}

struct Processor<R> where R: tokio::prelude::AsyncRead + tokio::prelude::AsyncBufRead + tokio::prelude::AsyncWrite + std::marker::Unpin {
    stream: R,
    storage: std::collections::HashMap<String, StoredValue>,
}

impl<R> Processor<R> where R: tokio::prelude::AsyncRead + tokio::prelude::AsyncBufRead + tokio::prelude::AsyncWrite + std::marker::Unpin {
    fn new(stream: R) -> Processor<R> {
        Processor{
            stream,
            storage: std::collections::HashMap::new(),
        }
    }

    async fn process_message(&mut self)-> io::Result<()> {
        let message = self.read_message().await?;
        let command = message.into_command();
        match command {
            Command::Ping => {
                self.send_response("+PONG\r\n").await?;
            }
            Command::Echo(data) => {
                self.send_response(&format!("+{}\r\n", data)).await?;
            }
            Command::Get(name) => {
                if let Some(StoredValue{value: Value::String(value), expiry}) = self.storage.get(&name) {
                    if let Some(expiry) = expiry {
                        if *expiry < std::time::Instant::now() {
                            return self.send_response("$-1\r\n").await;
                        }
                    }
                    let response = format!("+{}\r\n", value);
                    return self.send_response(&response).await;
                }
                self.send_response("$-1\r\n").await?;
            }
            Command::Set(name, value, expiry) => {
                self.storage.insert(name, StoredValue{value, expiry});
                self.send_response("+OK\r\n").await?;
            }
            Command::Error(cause) => {
                eprintln!("{}", cause);
            }
        }
        Ok(())
    }

    async fn send_response(&mut self, response: &str) -> io::Result<()> {
        self.stream.write_all(response.as_bytes()).await?;
        self.stream.flush().await?;
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
        match b as char {
            '*' => {
                let size = self.read_num::<usize>().await?;
                Ok(Value::Array(size, Vec::with_capacity(size)))
            }
            '$' => {
                let size = self.read_num::<i64>().await?;
                if size > 0 {
                    let result = self.read_fixed_string(size as usize).await?;
                    Ok(Value::String(result))
                } else {
                    Ok(Value::Nil)
                }
            }
            ':' => {
                let result = self.read_num::<i64>().await?;
                Ok(Value::Int(result))
            }
            _ => Ok(Value::Nil)
        }
    }

    async fn read_num<T>(&mut self) -> io::Result<T> where T: std::str::FromStr, <T as std::str::FromStr>::Err : std::fmt::Debug {
        let mut buf = vec![];
        self.stream.read_until(b'\n', &mut buf).await?;
        Ok(buf.iter().map(|b| *b as char).collect::<String>().trim().parse::<T>().unwrap())
    }

    async fn read_fixed_string(&mut self, size: usize) -> io::Result<String> {
        let mut result = vec![0; size];
        self.stream.read_exact(&mut result).await?;
        let result = result.iter().map(|b| *b as char).collect::<String>();

        let mut buf = vec![];
        self.stream.read_until(b'\n', &mut buf).await?;
        Ok(result)
    }
}

