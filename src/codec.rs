use std::io;
use std::collections::HashSet;
use std::net::SocketAddr;

use actix::prelude::*;
use byteorder::{BigEndian, ByteOrder};
use bytes::BytesMut;
use actix_codec::{Decoder, Encoder};



/// Codec for peer -> remote peer half
pub struct PeerConnectionCodec;

/// Implement decoder trait for P2P
impl Decoder for PeerConnectionCodec {
    type Item = Response;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        todo!()
    }
}

impl Encoder<Request> for PeerConnectionCodec {

    type Error = io::Error;

    fn encode(&mut self, item: Request, dst: &mut BytesMut) -> Result<(), Self::Error> {
        todo!()
    }

}

#[derive(Debug, Message)]
#[rtype(result = "()")]
pub enum Request {
    RandomMessage(RandomMessage),
    PeersRequest(PeersRequest),
}

#[derive(Debug, Message)]
#[rtype(result = "()")]
pub struct RandomMessage(pub String);

#[derive(Debug, Message)]
#[rtype(result = "Result<Peers, io::Error>")]
pub struct PeersRequest;

/// Remote peer response
#[derive(Debug, Message)]
#[rtype(result = "()")]
pub enum Response {
    Peers(Peers),
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