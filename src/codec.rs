use std::io;
use std::collections::HashSet;
use std::io::{ErrorKind, Read, Write};
use std::net::SocketAddr;
use std::str::FromStr;

use actix::prelude::*;
use byteorder::{BigEndian, ByteOrder, ReadBytesExt, WriteBytesExt};
use bytes::{Buf, BufMut, BytesMut};
use actix_codec::{Decoder, Encoder};
use bincode::{DefaultOptions, Options};
use bytes::buf::{Reader, Writer};
use serde_derive::{Deserialize, Serialize};
use tracing::{debug, error, info};
use crate::message::{InMessage, OutMessage};
use crate::message::Request::{MessageRequest, PeersRequest};
use crate::message::Response::{MessageResponse, PeersResponse};

/// Message [`actix::handler::Message`] flow:
///
/// Peer1 [`crate::peer::Peer`] actor, request [`ActorRequest`]
///         ->
/// Peer1 [`crate::connection::OutgoingConnection`] actor, request [`OutgoingNetworkRequest`]
///         ->
/// Peer2 [`crate::connection::IncomingConnection`] actor, request [`InMessage`]
///         ->
/// Peer2 [`crate::peer::Peer`] actor, response [`ActorRequest`]
///         ->
/// Peer2 [`crate::connection::OutgoingConnection`] actor, response [`OutgoingActorResponse`]
///         ->
/// Peer1 [`crate::connection::IncomingConnection`] actor, response [`OutMessage`]
///         ->
/// Peer1 [`crate::peer::Peer`] actor

/// Codec for [`crate::peer::OutgoingConnection`]
pub struct OutCodec;

impl Decoder for OutCodec {
    type Item = OutMessage;
    type Error = io::Error;

    //// read response from remote peer
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let mut reader = src.reader();
        let mut cmd_buf = [0u8; 1];
        reader.read(&mut cmd_buf)?;
        debug!("impl Decoder for OutCodec received command: [{}]", &cmd_buf[0]);
        match cmd_buf[0] {
            0 => {
                decode_msg(reader)
                    .map(|msg| {
                        Ok(Some(OutMessage::Response(MessageResponse(msg.msg, msg.sender))))
                    })
                    .map_err(|e| {
                        error!("decode_msg error : {e:?}");
                        io::Error::new(ErrorKind::InvalidInput, "Decode error")
                    })?
            }
            1 => {
                debug!("impl Decoder for OutCodec peers response");
                decode_peers(reader)
                    .map(|peers| {
                        Ok(Some(OutMessage::Response(PeersResponse(peers))))
                    })
                    .map_err(|e| {
                        error!("decode_peers error : {e:?}");
                        io::Error::new(ErrorKind::InvalidInput, "Decode error")
                    })?
            }
            _ => Err(io::Error::new(ErrorKind::InvalidInput, "Wrong command"))
        }
    }
}

/// Codec for [`crate::peer::IncomingConnection`]
pub struct InCodec;

impl Decoder for InCodec {
    type Item = InMessage;
    type Error = io::Error;

    /// Read request from remote peer
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        debug!("impl Decoder for InCodec");
        let mut reader = src.reader();
        let mut cmd_buf = [0u8; 1];
        reader.read(&mut cmd_buf)?;
        debug!("received command: [{}]", &cmd_buf[0]);
        match cmd_buf[0] {
            0 => {
                decode_msg(reader)
                    .map(|msg| {
                        Ok(Some(InMessage::Response(MessageResponse(msg.msg, msg.sender))))
                    })
                    .map_err(|e| {
                        error!("decode_msg error : {e:?}");
                        io::Error::new(ErrorKind::InvalidInput, "Decode error")
                    })?
            }
            1 => {
                debug!("impl Decoder for OutCodec peers response");
                decode_peers(reader)
                    .map(|peers| {
                        Ok(Some(InMessage::Response(PeersResponse(peers))))
                    })
                    .map_err(|e| {
                        error!("decode_peers error : {e:?}");
                        io::Error::new(ErrorKind::InvalidInput, "Decode error")
                    })?
            }
            _ => Err(io::Error::new(ErrorKind::InvalidInput, "Wrong command"))
        }
    }
}

impl Encoder<InMessage> for InCodec {
    type Error = io::Error;

    /// Send response to remote peer
    fn encode(&mut self, item: InMessage, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let mut writer = dst.writer();
        Ok(match item {
            InMessage::Request(req) => {
                match req {
                    crate::message::Request::MessageRequest(msg, addr) => {
                        debug!("codec encode random message");
                        let cmd_buf = [0u8; 1];
                        writer.write_all(&cmd_buf)?;
                        let msg = MessageWithSender { msg, sender: addr };
                        encode_and_write_msg(writer, &msg)?;
                    }
                    PeersRequest => {
                        debug!("codec encode peer request");
                        let cmd_buf = [1u8; 1];
                        writer.write_all(&cmd_buf)?;
                        writer.flush()?
                    }
                }
            }
            InMessage::Response(resp) => {
                match resp {
                    PeersResponse(peers) => {
                        encode_and_write_peers(writer, &peers)?
                    }
                    MessageResponse(..) => {
                        unreachable!()
                    }
                }
            }
        })
    }
}

impl Encoder<OutMessage> for InCodec {
    type Error = io::Error;

    /// Respond to incoming connection
    fn encode(&mut self, item: OutMessage, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let mut writer = dst.writer();
        Ok(match item {
            OutMessage::Request(req) => {
                match req {
                    crate::message::Request::MessageRequest(msg, addr) => {
                        debug!("codec encode random message");
                        let cmd_buf = [0u8; 1];
                        writer.write_all(&cmd_buf)?;
                        let msg = MessageWithSender { msg, sender: addr };
                        encode_and_write_msg(writer, &msg)?;
                    }
                    PeersRequest => {
                        debug!("codec encode peer request");
                        let cmd_buf = [1u8; 1];
                        writer.write_all(&cmd_buf)?;
                        writer.flush()?
                    }
                }
            }
            OutMessage::Response(resp) => {
                match resp {
                    PeersResponse(peers) => {
                        encode_and_write_peers(writer, &peers)?
                    }
                    MessageResponse(..) => {
                        unreachable!()
                    }
                }
            }
        })
    }
}

impl Encoder<OutMessage> for OutCodec {
    type Error = io::Error;

    /// Send response to remote peer
    fn encode(&mut self, item: OutMessage, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let mut writer = dst.writer();
        match item {
            OutMessage::Request(req) => {
                match req {
                    MessageRequest(msg, addr) => {
                        debug!("codec encode random message");
                        let cmd_buf = [0u8; 1];
                        writer.write_all(&cmd_buf)?;
                        let msg = MessageWithSender { msg, sender: addr };
                        Ok(encode_and_write_msg(writer, &msg)?)
                    }
                    PeersRequest => {
                        debug!("codec encode peer request");
                        let cmd_buf = [1u8; 1];
                        writer.write_all(&cmd_buf)?;
                        Ok(writer.flush()?)
                    }
                }
            }
            OutMessage::Response(resp) => {
                match resp {
                    PeersResponse(peers) => {
                        Ok(encode_and_write_peers(writer, &peers)?)
                    }
                    MessageResponse(..) => {
                        unreachable!()
                    }
                }
            }
        }
    }
}

impl Encoder<InMessage> for OutCodec {
    type Error = io::Error;

    fn encode(&mut self, item: InMessage, dst: &mut BytesMut) -> Result<(), Self::Error> {
        todo!()
    }
}

#[derive(Serialize, Deserialize)]
struct MessageWithSender {
    msg: String,
    sender: SocketAddr,
}

fn encode_and_write_msg(
    mut writer: Writer<&mut BytesMut>,
    msg: &MessageWithSender)
    -> io::Result<()> {
    debug!("codec encode random message");
    let byte_buf = bincode::serialize::<MessageWithSender>(&msg)
        .map_err(|_| io::Error::new(ErrorKind::InvalidInput, "Invalid encoding"))?;
    let len = byte_buf.len() as u32;
    writer.write_u32::<BigEndian>(len)?;
    writer.write_all(&byte_buf)?;
    Ok(writer.flush()?)
}

fn encode_and_write_peers(
    mut writer: Writer<&mut BytesMut>,
    peers: &HashSet<SocketAddr>)
    -> io::Result<()> {
    debug!("codec encode peers");
    let byte_buf = bincode::serialize::<HashSet<SocketAddr>>(&peers)
        .map_err(|_| io::Error::new(ErrorKind::InvalidInput, "Invalid encoding"))?;
    let len = byte_buf.len() as u32;
    writer.write_u32::<BigEndian>(len)?;
    Ok(writer.write_all(&byte_buf)?)
}

fn decode_msg(mut reader: Reader<&mut BytesMut>) -> io::Result<MessageWithSender> {
    debug!("codec decode random message");
    let size = reader.read_u32::<BigEndian>()?;
    let mut bytes_buf = vec![0_u8; size as usize];
    reader.read(&mut bytes_buf)?;
    let msg = deserialize_data(&bytes_buf)?;
    Ok(msg)
}

fn decode_peers(mut reader: Reader<&mut BytesMut>) -> io::Result<HashSet<SocketAddr>> {
    debug!("codec decode peer request");
    let size = reader.read_u32::<BigEndian>()?;
    let mut bytes_buf = vec![0_u8; size as usize];
    reader.read(&mut bytes_buf)?;
    let peers = deserialize_data(&bytes_buf)?;
    Ok(peers)
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


