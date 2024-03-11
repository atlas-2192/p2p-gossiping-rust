use std::io;
use std::collections::HashSet;
use std::io::{ErrorKind, Read, Write};
use std::net::SocketAddr;

use actix::prelude::*;
use byteorder::{BigEndian, ByteOrder, ReadBytesExt};
use bytes::{Buf, BufMut, BytesMut};
use actix_codec::{Decoder, Encoder};
use tracing::{debug, info};


/// Codec for peer -> remote peer half
pub struct PeerConnectionCodec;

/// Implement decoder trait for P2P
impl Decoder for PeerConnectionCodec {
    type Item = Response;
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
                Ok(Some(Response::Message(msg)))
            }
            1 => {
                debug!("Response peers", );
                Ok(Some(Response::Peers(Peers(Default::default()))))
            }
            _ => Err(io::Error::new(ErrorKind::InvalidInput, "Wrong command"))
        }
    }
}

impl Encoder<Request> for PeerConnectionCodec {

    type Error = io::Error;

    fn encode(&mut self, item: Request, dst: &mut BytesMut) -> Result<(), Self::Error> {
        match item {
            Request::RandomMessage(msg) => {
                let cmd_buf = [0u8; 1];
                let mut writer = dst.writer();
                let written = writer.write(&cmd_buf)?;
                debug!("written {:?} bytes to buff", written);
                writer.write_all(msg.msg.as_bytes())?;
                Ok(())
            }
            Request::PeersRequest(_req) => {
                let cmd_buf = [1u8; 1];
                let mut writer = dst.writer();
                let written = writer.write(&cmd_buf)?;
                debug!("written {:?} bytes to buff", written);
                Ok(())
            }
        }
    }

}

#[derive(Debug, Message)]
#[rtype(result = "()")]
pub enum Request {
    RandomMessage(Message),
    PeersRequest(PeersRequest),
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

/// Remote peer response
#[derive(Debug, Message)]
#[rtype(result = "()")]
pub enum Response {
    Peers(Peers),
    // TODO from
    Message(String)
}

#[derive(Debug, Message)]
#[rtype(result = "()")]
pub struct Peers (pub HashSet<SocketAddr>);


/// Codec for remote peer -> peer half
pub struct RemotePeerConnectionCodec;

impl Decoder for RemotePeerConnectionCodec {
    type Item = Request;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        Ok(None)
    }
}

impl Encoder<Response> for RemotePeerConnectionCodec {
    type Error = io::Error;

    fn encode(&mut self, msg: Response, dst: &mut BytesMut) -> Result<(), Self::Error> {
        Ok(())
    }
}