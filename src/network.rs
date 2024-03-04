use std::collections::HashSet;
use crate::error::{ConnectError, ConnectResult, RecvError, RecvResult, RequestError, RequestResult, SendResult};
use std::io::{Error, ErrorKind};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use bincode::{DefaultOptions, Options};
use tokio::io;
use tokio::net::{TcpListener, TcpStream, ToSocketAddrs};
use crate::error::RecvError::WrongCommand;

pub(crate) enum Message {
    Gossiping,
    PeersRequest,
}

impl TryFrom<u8> for Message {
    //type Error = RequestError;
    type Error = io::Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0u8 => Ok(Message::Gossiping),
            1u8 => Ok(Message::PeersRequest),
            // _ => Err(RequestError::Recv(WrongCommand))
            _ => Err(Error::new(ErrorKind::Other, "oh no try_from!"))
        }
    }
}

impl From<Message> for u8 {
    fn from(value: Message) -> Self {
        match value {
            Message::Gossiping => 0u8,
            Message::PeersRequest => 1u8,
        }
    }
}

pub(crate) async fn try_connect<A>(addrs: A) -> ConnectResult<TcpStream>
    where A: ToSocketAddrs
{
    let stream = TcpStream::connect(addrs).await?;
    try_handshake(stream).await
}

pub(crate) async fn accept_connection<A>(addrs: A) -> ConnectResult<TcpStream>
    where A: ToSocketAddrs
{
    let listener = TcpListener::bind(addrs).await?;
    let (stream, addr) = listener.accept().await?;
    println!("accepted connection from peer: {addr}");
    accept_handshake(stream).await
}

/// Intended to be used after ['network::try_connect()']
/// Peer sends all available peers except itself since connection has already been established
pub(crate) async fn request_peers(socket: &mut TcpStream) -> io::Result<HashSet<SocketAddr>> { //-> RequestResult<HashSet<SocketAddr>> {
    let cmd_byte = u8::from(Message::PeersRequest);
    let cmd_buf = [cmd_byte; 1];
    write_all_async(socket, &cmd_buf).await?;
    let mut len_buf = [0; 4];
    read_exact_async(socket, &mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf);
    let mut data_buf = vec![0; len as _];
    read_exact_async(socket, &mut data_buf).await?;
    // let peers = deserialize_data(&data_buf)?;
    if let Ok(peers) = deserialize_data(&data_buf) {
        return Ok(peers)
    } else {
        return Err(Error::new(ErrorKind::Other, "oh no request_peers!"))
    }

}

/// Intended to be used after ['network::try_connect()']
pub(crate) async fn respond_peers(socket: &mut TcpStream, peers: &HashSet<SocketAddr>) -> io::Result<()> { //-> SendResult {
    let data_buf = serialize_data(peers);
    let data_buf_len = (data_buf.len() as u32).to_be_bytes();
    write_all_async(socket, &data_buf_len).await?;
    write_all_async(socket, &data_buf).await?;
    Ok(())
}

/// Intended to be used after [`try_connect()`]
/// Write message to the socket after establishing connection with handshake
pub(crate) async fn write_msg<Data: AsRef<str>>(socket: &mut TcpStream, data: Data) -> io::Result<()> { // -> Result<(), RequestError> {
    let cmd_buf = [0u8; 1];
    write_all_async(socket, &cmd_buf).await?;
    let data_buf = data.as_ref().as_bytes();
    let data_buf_len = (data_buf.len() as u32).to_be_bytes();
    write_all_async(socket, &data_buf_len).await?;
    write_all_async(socket, data_buf).await?;
    Ok(())
}

pub(crate) async fn write_msg_arc<Data: AsRef<str>>(socket: Arc<Mutex<&TcpStream>>, data: Data) -> io::Result<()> { //  -> Result<(), RequestError> {
    let socket = socket.lock().unwrap();
    let cmd_buf = [0u8; 1];
    write_all_async(&*socket, &cmd_buf).await?;
    let data_buf = data.as_ref().as_bytes();
    let data_buf_len = (data_buf.len() as u32).to_be_bytes();
    write_all_async(&*socket, &data_buf_len).await?;
    write_all_async(&*socket, data_buf).await?;
    Ok(())
}

/// Intended to be used after ['network::accept_connection()']
/// Read message from the socket after establishing connection with handshake
pub(crate) async fn read_msg(socket: &mut TcpStream) -> io::Result<String> { // -> Result<String, RequestError> {
    let mut cmd_buf = [0u8; 1];
    read_exact_async(socket, &mut cmd_buf).await?;
    let mut len_buf = [0; 4];
    read_exact_async(socket, &mut len_buf).await?;
    // Let's think that message length is no longer u32
    let len = u32::from_be_bytes(len_buf);
    let mut data_buf = vec![0; len as _];
    read_exact_async(socket, &mut data_buf).await?;
    let msg = String::from_utf8(data_buf)
        .map_err(|e| Error::new(ErrorKind::InvalidInput, e))?;
    Ok(msg)
}

pub(crate) async fn write_all_async(stream: &TcpStream, buf: &[u8]) -> io::Result<()> {
    let mut written = 0;
    while written < buf.len() {
        stream.writable().await?;
        match stream.try_write(&buf[written..]) {
            Ok(0) => break,
            Ok(n) => {
                written += n;
            }
            Err(ref e) if e.kind() == ErrorKind::WouldBlock => { continue; }
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

pub(crate) async fn read_exact_async(s: &TcpStream, buf: &mut [u8]) -> io::Result<()> {
    let mut red = 0;
    while red < buf.len() {
        s.readable().await?;
        match s.try_read(&mut buf[red..]) {
            Ok(0) => break,
            Ok(n) => {
                red += n;
            }
            Err(ref e) if e.kind() == ErrorKind::WouldBlock => { continue; }
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

async fn try_handshake(mut stream: TcpStream) -> ConnectResult<TcpStream> {
    write_all_async(&mut stream, b"req").await?;
    let mut buf = [0; 4];
    write_all_async(&mut stream, &mut buf).await?;
    if &buf != b"resp" {
        let msg = format!("received: {:?}", buf);
        return Err(ConnectError::BadHandshake(msg));
    }
    Ok(stream)
}

async fn accept_handshake(stream: TcpStream) -> ConnectResult<TcpStream> {
    let mut buf = [0; 3];
    read_exact_async(&stream, &mut buf).await?;
    if &buf != b"req" {
        let msg = format!("received: {:?}", buf);
        return Err(ConnectError::BadHandshake(msg));
    }
    write_all_async(&stream, b"resp").await?;
    Ok(stream)
}

fn serialize_data<DATA: serde::ser::Serialize>(data: DATA) -> Vec<u8> {
    DefaultOptions::new()
        .with_varint_encoding()
        .serialize(&data).unwrap()
}

fn deserialize_data<'a, DATA: serde::de::Deserialize<'a>>(bytes:  &'a [u8])
    -> Result<DATA, RequestError> {
    let data = DefaultOptions::new()
        .with_varint_encoding()
        .deserialize::<DATA>(&bytes[..]);
    if let Ok(data) = data {
        Ok(data)
    } else {
        let err = data.err().unwrap();
        println!("data deserialization error: {}",  err);
        Err(RequestError::Recv(RecvError::BadEncoding))
    }
}
