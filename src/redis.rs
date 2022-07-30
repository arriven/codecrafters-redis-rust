use std::io;
use std::convert::TryInto;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt, AsyncBufReadExt};

#[derive(Debug)]
pub enum Error {
    Io(std::io::Error),
    Argument(String),
    TryFromInt(std::num::TryFromIntError),
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<std::num::TryFromIntError> for Error {
    fn from(e: std::num::TryFromIntError) -> Self {
        Self::TryFromInt(e)
    }
}

#[derive(Clone)]
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
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Value::Array(size, data) => write!(f, "*{}{}\r\n", size, data.iter().map(std::string::ToString::to_string).collect::<String>()),
            Value::String(data) => write!(f, "${}\r\n{}\r\n", data.as_bytes().len(), data),
            Value::Int(n) => write!(f, ":{}\r\n", n),
            Value::Nil => write!(f, "$-1\r\n"),
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
                        duration.try_into()?
                    } else if let Value::String(duration) = arg {
                        duration.parse::<u64>().unwrap()
                    } else {
                        return Err(Error::Argument("SET: wrong argument type".to_owned()));
                    };
                    let expiry = std::time::Instant::now() + std::time::Duration::from_millis(duration);
                    Command::set(data, Some(expiry))
                },
                _ => Err(Error::Argument(format!("SET: flag not implemented: {}", flag)))
            }
        } else {
            Err(Error::Argument("SET: wrong argument type".to_owned()))
        }
    }
}

pub struct StoredValue {
    value: Value,
    expiry: Option<std::time::Instant>,
}

pub struct Server {
    storage: Storage,
}

impl Server {
    pub fn new() -> Server {
        let storage = Arc::new(Mutex::new(std::collections::HashMap::new()));
        {
            let storage = storage.clone();
            tokio::spawn(async move {
                Server::gc(storage).await;
            });
        }
        Server{
            storage,
        }
    }

    pub fn worker<R>(&self, stream: R) -> Worker<R> where R: tokio::prelude::AsyncRead + tokio::prelude::AsyncBufRead + tokio::prelude::AsyncWrite + std::marker::Unpin {
        Worker{
            stream,
            storage: self.storage.clone(),
        }
    }

    async fn gc(storage: Storage) {
        loop {
            storage.lock().await.retain(|_, ref mut v| {
                if let Some(expiry) = v.expiry {
                    return expiry < std::time::Instant::now();
                }
                true
            });
        }
    }
}

type Storage = Arc<Mutex<std::collections::HashMap<String, StoredValue>>>;

pub struct Worker<R> where R: tokio::prelude::AsyncRead + tokio::prelude::AsyncBufRead + tokio::prelude::AsyncWrite + std::marker::Unpin {
    stream: R,
    storage: Storage,
}

impl<R> Worker<R> where R: tokio::prelude::AsyncRead + tokio::prelude::AsyncBufRead + tokio::prelude::AsyncWrite + std::marker::Unpin {
    pub async fn run(mut self) -> Result<(), Error> {
        loop {
            self.process_message().await?;
        }
    }

    pub async fn process_message(&mut self) -> Result<(), Error> {
        let message = self.read_message().await?;
        let command = Command::from_value(message)?;
        let response = match command {
            Command::Ping => Value::String("PONG".to_string()),
            Command::Echo(data) => Value::String(data),
            Command::Get(name) => self.get_value(&name).await,
            Command::Set(name, value, expiry) => {
                self.storage.lock().await.insert(name, StoredValue{value, expiry});
                Value::String("OK".to_string())
            }
        }.to_string();
        self.send_response(&response).await
    }

    async fn get_value(&self, name: &str) -> Value {
        if let Some(StoredValue{value, expiry}) = self.storage.lock().await.get(name) {
            if let Some(expiry) = expiry {
                if *expiry < std::time::Instant::now() {
                    return Value::Nil;
                }
            }
            return value.clone()
        }
        Value::Nil
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
                    let result = self.read_fixed_string(size.try_into()?).await?;
                    Ok(Value::String(result))
                } else {
                    Ok(Value::Nil)
                }
            }
            ':' => {
                let result = self.read_num::<i64>().await?;
                Ok(Value::Int(result))
            }
            '+' => {
                let result = self.read_simple_string().await?;
                Ok(Value::String(result))
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

    async fn read_simple_string(&mut self) -> io::Result<String> {
        let mut buf = vec![];
        self.stream.read_until(b'\n', &mut buf).await?;
        Ok(buf.iter().map(|b| *b as char).collect::<String>().trim().to_string())
    }
}

