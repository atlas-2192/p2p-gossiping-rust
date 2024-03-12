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


/// Codec for [`crate::peer::ToRemoteConnection`]
pub struct ToRemoteCodec;

/// Request to remote peer
#[derive(Debug, Message, Clone)]
#[rtype(result = "()")]
pub enum RequestToRemote {
    /// send random message to remote peer
    RandomMessage(String, SocketAddr),
    /// request remote peer for all active peers in network
    PeersRequest,
}

/// Remote peer response
#[derive(Debug, Message)]
#[rtype(result = "()")]
pub enum ResponseFromRemote {
    /// Response to [`RequestToRemote::PeersRequest`] from remote peer
    Peers(HashSet<SocketAddr>),
    /// Placeholder for empty response
    Empty,
}


impl Decoder for ToRemoteCodec {
    type Item = ResponseFromRemote;
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
                Ok(Some(ResponseFromRemote::Empty))
            }
            1 => {
                debug!("codec decode peers response");
                let size = reader.read_u32::<BigEndian>()?;
                let mut bytes_buf = vec![0_u8; size as usize];
                let red = reader.read(&mut bytes_buf)?;
                debug!("{red} bytes red , bytes: {:?}", &bytes_buf);
                let peers = deserialize_data::<HashSet<SocketAddr>>(&bytes_buf)?;
                Ok(Some(ResponseFromRemote::Peers(peers)))
            }
            _ => Err(io::Error::new(ErrorKind::InvalidInput, "Wrong command"))
        }
    }
}

impl Encoder<RequestToRemote> for ToRemoteCodec {

    type Error = io::Error;

    //// send request to remote peer
    fn encode(&mut self, item: RequestToRemote, dst: &mut BytesMut) -> Result<(), Self::Error> {
        match item {
            RequestToRemote::RandomMessage(msg, addr) => {
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
            RequestToRemote::PeersRequest => {
                debug!("codec encode peer request");
                let cmd_buf = [1u8; 1];
                let mut writer = dst.writer();
                writer.write_all(&cmd_buf)?;
                // TEST
                // error: Slice had bytes remaining after deserialization
                // let byte_buf = bincode::serialize::<HashSet<SocketAddr>>(&HashSet::new()).unwrap();
                // let len = byte_buf.len() as u32;
                // writer.write_u32::<BigEndian>(len)?;
                // writer.write_all(&byte_buf)?;
                Ok(())
            }
        }
    }
}

/// Codec for [`crate::peer::FromRemoteConnection`]
pub struct FromRemoteCodec;

/// Request from remote peer
#[derive(Debug, Message)]
#[rtype(result = "()")]
pub enum RequestFromRemote {
    /// send random message to peer
    RandomMessage(String, SocketAddr),
    /// request all active peers in network
    PeersRequest,
}

/// response to remote peer
#[derive(Debug, Message)]
#[rtype(result = "()")]
pub enum ResponseToRemote {
    /// Response to [`RequestFromRemote::PeersRequest`] to remote peer
    Peers(HashSet<SocketAddr>),
    /// Placeholder for empty response
    Empty,
}

impl Decoder for FromRemoteCodec {
    type Item = ResponseToRemote;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        debug!("Decoder for FromRemoteCodec");
        Ok(None)
    }
}

impl Encoder<RequestToRemote> for FromRemoteCodec {
    type Error = io::Error;

    fn encode(&mut self, msg: RequestToRemote, dst: &mut BytesMut) -> Result<(), Self::Error> {
        debug!("Encoder<RequestToRemote> for FromRemoteCodec");
        Ok(())
    }
}

fn deserialize_data<'a, DATA: serde::de::Deserialize<'a>>(bytes:  &'a [u8])
    -> Result<DATA, io::Error> {
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


