use std::io;
use std::collections::HashSet;
use std::io::{ErrorKind, Read, Write};
use std::net::SocketAddr;

use actix::prelude::*;
use byteorder::{BigEndian, ByteOrder, ReadBytesExt};
use bytes::{Buf, BufMut, BytesMut};
use actix_codec::{Decoder, Encoder};
use tracing::{debug, info};

pub trait Codec {}

impl Codec for RemoteToLocalCodec {}

impl Codec for LocalToRemoteCodec {}

/// Codec for [`crate::peer::RemoteToLocalConnection`]
pub struct RemoteToLocalCodec;

impl Decoder for RemoteToLocalCodec {
    type Item = ResponseFromRemote;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let mut reader = src.reader();
        let mut cmd_buf = [0u8; 1];
        let red = reader.read(&mut cmd_buf)?;
        debug!("red {red} byte from buff, command: [{}]", &cmd_buf[0]);
        match cmd_buf[0] {
            0 => {
                let size = reader.read_u32::<BigEndian>()?;
                let mut msg_buf = vec![0_u8; size as usize];
                let red = reader.read(&mut msg_buf)?;
                debug!("{red} bytes red , bytes: {:?}", &msg_buf);
                let msg = String::from_utf8(msg_buf)
                    .map_err(|_| io::Error::new(ErrorKind::InvalidInput, "Invalid utf8"))?;
                debug!("Received message [{}]", &msg);
                Ok(Some(ResponseFromRemote::Peers(Peers(Default::default()))))
            }
            1 => {
                debug!("Response peers", );
                Ok(Some(ResponseFromRemote::Peers(Peers(Default::default()))))
            }
            _ => Err(io::Error::new(ErrorKind::InvalidInput, "Wrong command"))
        }
    }
}

impl Encoder<RequestToRemote> for RemoteToLocalCodec {

    type Error = io::Error;

    fn encode(&mut self, item: RequestToRemote, dst: &mut BytesMut) -> Result<(), Self::Error> {
        match item {
            RequestToRemote::RandomMessage(msg) => {
                let cmd_buf = [0u8; 1];
                let mut writer = dst.writer();
                let written = writer.write(&cmd_buf)?;
                debug!("written {:?} bytes to buff", written);
                writer.write_all(msg.msg.as_bytes())?;
                Ok(())
            }
            RequestToRemote::PeersRequest(_req) => {
                let cmd_buf = [1u8; 1];
                let mut writer = dst.writer();
                let written = writer.write(&cmd_buf)?;
                debug!("written {:?} bytes to buff", written);
                Ok(())
            }
        }
    }
}

impl Encoder<RequestFromRemote> for RemoteToLocalCodec {
    type Error = io::Error;

    fn encode(&mut self, item: RequestFromRemote, dst: &mut BytesMut) -> Result<(), Self::Error> {
        todo!()
    }
}

/// Request to remote peer
#[derive(Debug, Message)]
#[rtype(result = "()")]
pub enum RequestToRemote {
    RandomMessage(Message),
    PeersRequest(PeersRequest),
}

/// Remote peer response
#[derive(Debug, Message)]
#[rtype(result = "()")]
pub enum ResponseFromRemote {
    Peers(Peers),
}

/// request from remote peer to local peer
#[derive(Debug, Message)]
#[rtype(result = "()")]
pub enum RequestFromRemote {
    RandomMessage(Message),
    PeersRequest(PeersRequest),
}

/// response from local peer to remote peer
#[derive(Debug, Message)]
#[rtype(result = "()")]
pub enum ResponseToRemote {
    Peers(Peers),
}

#[derive(Debug, Message)]
#[rtype(result = "()")]
pub struct Message {
    pub msg: String,
    pub from: SocketAddr,
}

#[derive(Debug, Message)]
#[rtype(result = "Result<Peers, io::Error>")]
pub struct PeersRequest;

#[derive(Debug, Message)]
#[rtype(result = "()")]
pub enum LocalResponse {
    Peers(Peers)
}

#[derive(Debug, Message)]
#[rtype(result = "()")]
pub struct Peers (pub HashSet<SocketAddr>);


/// Codec for [`crate::peer::LocalToRemoteConnection`]
pub struct LocalToRemoteCodec;

impl Decoder for LocalToRemoteCodec {
    type Item = ResponseToRemote;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        Ok(None)
    }
}

impl Encoder<RequestToRemote> for LocalToRemoteCodec {
    type Error = io::Error;

    fn encode(&mut self, msg: RequestToRemote, dst: &mut BytesMut) -> Result<(), Self::Error> {
        Ok(())
    }
}
