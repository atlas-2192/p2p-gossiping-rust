use std::io;
use std::collections::HashSet;
use std::io::{ErrorKind, Read, Write};
use std::net::SocketAddr;

use actix::prelude::*;
use byteorder::{BigEndian, ByteOrder, ReadBytesExt, WriteBytesExt};
use bytes::{Buf, BufMut, BytesMut};
use actix_codec::{Decoder, Encoder};
use bincode::{DefaultOptions, Options};
use serde_derive::{Deserialize, Serialize};
use tracing::{debug, error, info};


/// Codec for [`crate::peer::P2PConnection`]
pub struct P2PCodec;

impl Decoder for P2PCodec {
    type Item = Response;
    type Error = io::Error;

    //// read response from remote peer
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let mut reader = src.reader();
        let mut cmd_buf = [0u8; 1];
        let red = reader.read(&mut cmd_buf)?;
        debug!("red {red} byte from buff, command: [{}]", &cmd_buf[0]);
        match cmd_buf[0] {
            0 => {
                debug!("codec decode random message");
                let size = reader.read_u32::<BigEndian>()?;
                let mut bytes_buf = vec![0_u8; size as usize];
                let red = reader.read(&mut bytes_buf)?;
                debug!("{red} bytes red , bytes: {:?}", &bytes_buf);
                let msg = String::from_utf8(bytes_buf)
                    .map_err(|_| io::Error::new(ErrorKind::InvalidInput, "Invalid utf8"))?;
                debug!("Received message [{}]", &msg);
                Ok(Some(Response::Empty))
            }
            1 => {
                debug!("codec decode peers response");
                let size = reader.read_u32::<BigEndian>()?;
                let mut bytes_buf = vec![0_u8; size as usize];
                let red = reader.read(&mut bytes_buf)?;
                debug!("{red} bytes red , bytes: {:?}", &bytes_buf);
                let peers = deserialize_data::<HashSet<SocketAddr>>(&bytes_buf)?;
                Ok(Some(Response::Peers(peers)))
            }
            _ => Err(io::Error::new(ErrorKind::InvalidInput, "Wrong command"))
        }
    }
}

impl Encoder<Request> for P2PCodec {

    type Error = io::Error;

    //// send request to remote peer
    fn encode(&mut self, item: Request, dst: &mut BytesMut) -> Result<(), Self::Error> {
        match item {
            Request::RandomMessage(msg, addr) => {
                debug!("codec encode random message");
                let cmd_buf = [0u8; 1];
                let mut writer = dst.writer();
                writer.write_all(&cmd_buf)?;
                let byte_buf = msg.as_bytes();
                let len = byte_buf.len() as u32;
                writer.write_u32::<BigEndian>(len)?;
                writer.write_all(&byte_buf)?;
                Ok(())
            }
            Request::PeersRequest => {
                debug!("codec encode peer request");
                let cmd_buf = [1u8; 1];
                let mut writer = dst.writer();
                writer.write_all(&cmd_buf)?;
                Ok(())
            }
        }
    }
}

/// Request to remote peer
#[derive(Debug, Message, Clone)]
#[rtype(result = "()")]
pub enum Request {
    /// send random message to peer
    RandomMessage(String, SocketAddr),
    /// request all active peers in network
    PeersRequest,
}

/// Remote peer response
#[derive(Debug, Message)]
#[rtype(result = "()")]
pub enum Response {
    /// Response to [`Request::PeersRequest`]
    Peers(HashSet<SocketAddr>),
    /// Placeholder for empty response
    Empty,
}

/// Message sent to all the peers in network
// #[derive(Debug, Message, Clone, Serialize, Deserialize)]
// #[rtype(result = "()")]
// pub struct Message {
//     pub msg: String,
//     /// In order to simplify remote peer recognition
//     /// TODO change 127.0.0.1:54323 -> 127.0.0.1:8080
//     pub from: SocketAddr,
// }
//
// #[derive(Debug, Message, Clone)]
// #[rtype(result = "Result<Peers, io::Error>")]
// pub struct PeersRequest;
//
// #[derive(Debug, Message)]
// #[rtype(result = "()")]
// pub struct Peers (pub HashSet<SocketAddr>);

pub fn deserialize_data<'a, DATA: serde::de::Deserialize<'a>>(bytes:  &'a [u8])
    -> Result<DATA, io::Error>
{
    let data = DefaultOptions::new()
        .with_varint_encoding()
        .deserialize::<DATA>(&bytes[..]);
    if let Ok(data) = data {
        Ok(data)
    } else {
        let err = data.err().unwrap();
        error!("network::deserialize_data() error: {}",  err);
        Err(io::Error::new(ErrorKind::InvalidData, "deserialization error"))
    }
}


// /// Request to remote peer
// #[derive(Debug, Message)]
// #[rtype(result = "()")]
// pub enum RequestToRemote {
//     RandomMessage(Message),
//     PeersRequest(PeersRequest),
// }
//
// /// Remote peer response
// #[derive(Debug, Message)]
// #[rtype(result = "()")]
// pub enum ResponseFromRemote {
//     Peers(Peers),
// }
//
// /// request from remote peer to local peer
// #[derive(Debug, Message)]
// #[rtype(result = "()")]
// pub enum RequestFromRemote {
//     RandomMessage(Message),
//     PeersRequest(PeersRequest),
// }
//
// /// response from local peer to remote peer
// #[derive(Debug, Message)]
// #[rtype(result = "()")]
// pub enum ResponseToRemote {
//     Peers(Peers),
// }

// #[derive(Debug, Message)]
// #[rtype(result = "()")]
// pub enum LocalResponse {
//     Peers(Peers)
// }


// /// Codec for [`crate::peer::LocalToRemoteConnection`]
// pub struct LocalToRemoteCodec;
//
// impl Decoder for LocalToRemoteCodec {
//     type Item = ResponseToRemote;
//     type Error = io::Error;
//
//     fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
//         Ok(None)
//     }
// }
//
// impl Encoder<RequestToRemote> for LocalToRemoteCodec {
//     type Error = io::Error;
//
//     fn encode(&mut self, msg: RequestToRemote, dst: &mut BytesMut) -> Result<(), Self::Error> {
//         Ok(())
//     }
// }

