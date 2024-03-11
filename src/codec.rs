use std::io;
use std::collections::HashSet;
use std::net::SocketAddr;

use actix::prelude::*;
use byteorder::{BigEndian, ByteOrder};
use bytes::BytesMut;
use actix_codec::{Decoder, Encoder};
use serde::{Deserialize, Serialize};
use serde_derive::{Deserialize, Serialize};
//use tokio_util::codec::{Decoder, Encoder};
//use tokio_io::_tokio_codec::{Decoder, Encoder};
use crate::peer::Peer;

/// Message coming from the network
// #[derive(Debug, Message)]
// #[rtype(result = "()")]
// pub enum Request {
//     RandomMessage(String),
//     #[rtype(result = "()")]
//     PeersRequest(PeersRequest),
// }
//
// /// Message going to the network
// #[derive(Debug, Message)]
// #[rtype(result = "()")]
// pub enum Response {
//     Message(String),
// }

/// Codec for Client -> Server transport
pub struct PeerConnectionCodec;

/// Implement decoder trait for P2P
impl Decoder for PeerConnectionCodec {
    type Item = Response;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        todo!()
    }
}

/// Codec for peer -> remote peer half
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
    PeersRequest,
}

#[derive(Debug, Message)]
#[rtype(result = "()")]
struct RandomMessage(String);

#[derive(Message)]
#[rtype(result = "Peers")]
struct PeersRequest;

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