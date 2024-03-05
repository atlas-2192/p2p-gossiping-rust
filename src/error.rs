use std::io;
use thiserror::Error;

pub type ConnectResult<T> = Result<T, ConnectError>;

#[derive(Debug, Error)]
pub enum ResolverError {
    #[error("Failed resolving hostname: {}", _0)]
    Resolver(String),

    /// Address is invalid
    #[error("Invalid input: {}", _0)]
    InvalidInput(&'static str),

    /// Connecting took too long
    #[error("Timeout out while establishing connection")]
    Timeout,

    /// Connection io error
    #[error("{}", _0)]
    IoError(#[from] io::Error),
}

/// Connection error. Includes IO and handshake error.
#[derive(Debug, Error)]
pub enum ConnectError {
    #[error("Unexpected handshake response: {0}")]
    BadHandshake(String),
    #[error("IO error: {0}")]
    Io(#[from] io::Error),
}

pub type SendResult = Result<(), SendError>;

/// Send data error
#[derive(Debug, Error)]
pub enum SendError {
    #[error("IO error: {0}")]
    Io(#[from] io::Error),
}

pub type RecvResult<T> = Result<T, RecvError>;

/// Send data error. Includes IO and encoding error.
#[derive(Debug, Error)]
pub enum RecvError {
    #[error("IO error: {0}")]
    Io(#[from] io::Error),
    #[error("bad encoding")]
    BadEncoding,
    #[error("wrong command")]
    WrongCommand,
}

pub type RequestResult<T> = Result<T, RequestError>;

/// Error for request sending. It consists from two steps: sending and receiving data.
///
/// `SendError` caused by send data error.
/// `RecvError` caused by receive data error.
#[derive(Debug, Error)]
pub enum RequestError {
    #[error(transparent)]
    Send(#[from] SendError),
    #[error(transparent)]
    Recv(#[from] RecvError),
}