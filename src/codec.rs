use std::io;
use std::io::{ErrorKind};

use actix::prelude::*;
use byteorder::{BigEndian, ByteOrder};
use bytes::{Buf, BufMut, BytesMut};
use actix_codec::{Decoder, Encoder};
use bincode::{DefaultOptions, Options};
use tracing::{debug, error, info, warn};
use crate::message::{InMessage, OutMessage};


/// Message [`actix::handler::Message`] flow:
///
/// Peer1 [`crate::peer::Peer`] actor, request [`ActorRequest`]
///         ->
/// Peer1 [`crate::connection::OutConnection`] actor, request [`OutgoingNetworkRequest`]
///         ->
/// Peer2 [`crate::connection::InConnection`] actor, request [`InMessage`]
///         ->
/// Peer2 [`crate::peer::Peer`] actor, response [`ActorRequest`]
///         ->
/// Peer2 [`crate::connection::OutConnection`] actor, response [`OutgoingActorResponse`]
///         ->
/// Peer1 [`crate::connection::InConnection`] actor, response [`OutMessage`]
///         ->
/// Peer1 [`crate::peer::Peer`] actor

/// Codec for [`crate::peer::OutConnection`]
pub struct OutCodec;

impl Decoder for OutCodec {
    type Item = OutMessage;
    type Error = io::Error;

    //// read response from remote peer
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let size = {
            if src.len() < 2 {
                return Ok(None);
            }
            BigEndian::read_u16(src.as_ref()) as usize
        };

        if src.len() >= size + 2 {
            let _ = src.split_to(2);
            let buf = src.split_to(size);
            Ok(Some(deserialize_data::<OutMessage>(&buf)?))
        } else {
            Ok(None)
        }
    }
}

/// Codec for [`crate::peer::InConnection`]
pub struct InCodec;

impl Decoder for InCodec {
    type Item = InMessage;
    type Error = io::Error;

    /// Read request from remote peer
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let size = {
            if src.len() < 2 {
                return Ok(None);
            }
            BigEndian::read_u16(src.as_ref()) as usize
        };

        if src.len() >= size + 2 {
            let _ = src.split_to(2);
            let buf = src.split_to(size);
            Ok(Some(deserialize_data::<InMessage>(&buf)?))
        } else {
            Ok(None)
        }
    }
}

impl Encoder<InMessage> for InCodec {
    type Error = io::Error;

    /// Respond to incoming connection
    fn encode(&mut self, item: InMessage, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let msg = serialize_data(&item).unwrap();
        let msg_ref: &[u8] = msg.as_ref();

        dst.reserve(msg_ref.len() + 2);
        dst.put_u16(msg_ref.len() as u16);
        dst.put(msg_ref);

        Ok(())
    }
}

impl Encoder<OutMessage> for InCodec {
    type Error = io::Error;

    /// Respond to incoming connection
    fn encode(&mut self, item: OutMessage, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let msg = serialize_data(&item).unwrap();
        let msg_ref: &[u8] = msg.as_ref();

        dst.reserve(msg_ref.len() + 2);
        dst.put_u16(msg_ref.len() as u16);
        dst.put(msg_ref);

        Ok(())
    }
}

impl Encoder<OutMessage> for OutCodec {
    type Error = io::Error;

    fn encode(&mut self, item: OutMessage, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let msg = serialize_data(&item).unwrap();
        let msg_ref: &[u8] = msg.as_ref();

        dst.reserve(msg_ref.len() + 2);
        dst.put_u16(msg_ref.len() as u16);
        dst.put(msg_ref);

        Ok(())
    }
}

pub fn serialize_data<DATA: serde::ser::Serialize>(data: DATA) -> io::Result<Vec<u8>> {
    Ok(DefaultOptions::new()
        .with_varint_encoding()
        .serialize(&data)
        .map_err(|e| io::Error::new(ErrorKind::InvalidInput, format!("serialization error: {e}")))?)
}

fn deserialize_data<'a, DATA: serde::de::Deserialize<'a>>(bytes:  &'a [u8])
    -> Result<DATA, io::Error> {
    let data = DefaultOptions::new()
        .with_varint_encoding()
        .deserialize::<DATA>(&bytes[..]);
    if let Ok(data) = data {
        Ok(data)
    } else {
        let err = format!("deserialization error: {}",  data.err().unwrap());
        Err(io::Error::new(ErrorKind::InvalidData, err))
    }
}


