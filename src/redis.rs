use std::io;
use tokio::io::{AsyncReadExt, AsyncWriteExt, AsyncBufReadExt};

#[derive(Debug)]
pub enum Error {
    Io(std::io::Error),
    Argument(String),
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
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

    fn to_string(&self) -> String {
        match self {
            Value::Array(size, data) => format!("*{}{}\r\n", size, data.iter().map(|v| v.to_string()).collect::<Vec<_>>().join("")),
            Value::String(data) => format!("${}\r\n{}\r\n", data.as_bytes().len(), data),
            Value::Int(n) => format!(":{}\r\n", n),
            Value::Nil => "$-1\r\n".to_string(),
        }
    }
}

enum Command {
    Ping,
    Echo(String),
    Get(String),
    Set(String, Value, Option<std::time::Instant>),
}

impl Command {
    fn from_value(value: Value) -> Result<Command, Error> {
        match value {
            Value::Array(_, data) => Command::from_array(data),
            Value::String(data) => Command::from_string(&data),
            _ => Err(Error::Argument("wrong argument type".to_owned())),
        }
    }

    fn from_string(data: &str) -> Result<Command, Error> {
        match data.to_lowercase().as_str() {
            "ping" => Ok(Command::Ping),
            _ => Err(Error::Argument(format!("not implemented: {}", data))),
        }
    }

    fn from_array(data: Vec<Value>) -> Result<Command, Error> {
        if data.is_empty() {
            return Err(Error::Argument("empty command".to_owned()));
        }

        match &data[0] {
            Value::String(command) => match command.to_lowercase().as_str() {
                "ping" => Ok(Command::Ping),
                "echo" => Command::echo(data),
                "get" => Command::get(data),
                "set" => Command::set(data, None),
                _ => Err(Error::Argument(format!("not implemented: {}", command))),
            },
            _ => Err(Error::Argument("wrong argument type".to_owned())),
        }
    }

    fn echo(mut data: Vec<Value>) -> Result<Command, Error> {
        if let Some(Value::String(data)) = data.pop() {
            Ok(Command::Echo(data))
        } else {
            Err(Error::Argument("ECHO: wrong argument type".to_owned()))
        }
    }

    fn get(mut data: Vec<Value>) -> Result<Command, Error> {
        if let Some(Value::String(data)) = data.pop() {
            Ok(Command::Get(data))
        } else {
            Err(Error::Argument("GET: wrong argument type".to_owned()))
        }
    }

    fn set(mut data: Vec<Value>, expiry: Option<std::time::Instant>) -> Result<Command, Error> {
        if data.len() < 3 {
            return Err(Error::Argument(format!{"not enough arguments for set: {}", data.len()}))
        }
        if data.len() > 3 {
            return Command::set_with_flags(data)
        }

        let value = data.pop().unwrap();
        if let Value::String(name) = data.pop().unwrap() {
            Ok(Command::Set(name, value, expiry))
        } else {
            Err(Error::Argument("SET: wrong argument type".to_owned()))
        }
    }

    fn set_with_flags(mut data: Vec<Value>) -> Result<Command, Error> {
        let arg = data.pop().unwrap();
        if let Value::String(flag) = data.pop().unwrap() {
            match flag.to_lowercase().as_str() {
                "px" => {
                    let duration = if let Value::Int(duration) = arg {
                        duration as u64
                    } else if let Value::String(duration) = arg {
                        duration.parse::<u64>().unwrap()
                    } else {
                        return Err(Error::Argument("SET: wrong argument type".to_owned()));
                    };
                    let expiry = std::time::Instant::now() + std::time::Duration::from_millis(duration as u64);
                    Command::set(data, Some(expiry))
                },
                _ => Err(Error::Argument(format!("SET: flag not implemented: {}", flag)))
            }
        } else {
            Err(Error::Argument("SET: wrong argument type".to_owned()))
        }
    }
}

struct StoredValue {
    value: Value,
    expiry: Option<std::time::Instant>,
}

pub struct Server<R> where R: tokio::prelude::AsyncRead + tokio::prelude::AsyncBufRead + tokio::prelude::AsyncWrite + std::marker::Unpin {
    stream: R,
    storage: std::collections::HashMap<String, StoredValue>,
}

impl<R> Server<R> where R: tokio::prelude::AsyncRead + tokio::prelude::AsyncBufRead + tokio::prelude::AsyncWrite + std::marker::Unpin {
    pub fn new(stream: R) -> Server<R> {
        Server{
            stream,
            storage: std::collections::HashMap::new(),
        }
    }

    pub async fn process_message(&mut self)-> Result<(), Error> {
        let message = self.read_message().await?;
        let command = Command::from_value(message)?;
        match command {
            Command::Ping => self.send_response(&Value::String("PONG".to_string()).to_string()).await,
            Command::Echo(data) => self.send_response(&Value::String(data).to_string()).await,
            Command::Get(name) => {
                if let Some(StoredValue{value, expiry}) = self.storage.get(&name) {
                    if let Some(expiry) = expiry {
                        if *expiry < std::time::Instant::now() {
                            return self.send_response(&Value::Nil.to_string()).await;
                        }
                    }
                    let response = value.to_string();
                    return self.send_response(&response).await;
                }
                self.send_response(&Value::Nil.to_string()).await
            }
            Command::Set(name, value, expiry) => {
                self.storage.insert(name, StoredValue{value, expiry});
                self.send_response(&Value::String("OK".to_string()).to_string()).await
            }
        }
    }

    async fn send_response(&mut self, response: &str) -> Result<(), Error> {
        self.stream.write_all(response.as_bytes()).await?;
        self.stream.flush().await?;
        Ok(())
    }

    async fn read_message(&mut self) -> Result<Value, Error> {
        let mut message = self.read_single().await?;
        while !message.is_complete() {
            let next = self.read_single().await?;
            message.append(next);
        }
        Ok(message)
    }

    async fn read_single(&mut self) -> Result<Value, Error> {
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

