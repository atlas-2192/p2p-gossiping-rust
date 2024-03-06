use bytes::BytesMut;
use std::io;
use actix::Message;
use tokio_util::codec::{Encoder, Decoder};

/// Message coming from the network
#[derive(Debug, Message)]
#[rtype(result = "()")]
pub enum Request {
    Message(String),
}

/// Message going to the network
#[derive(Debug, Message)]
#[rtype(result = "String")]
pub enum Response {
    Message(String),
}

/// Codec for p2p transport
pub struct P2PCodec;

/// Implement decoder trait for P2P
impl Decoder for P2PCodec {
    type Item = Request;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {

        // Locate a byte corresponding to a '\n' in the byte stream
        if let Some(i) = src.iter().position(|&b| b == b'\n') {
            // Remove the serialized frame from the buffer.
            let line = src.split_to(i + 1);

            // Parse the buffer as an UTF-8 encoded string
            let mut res = String::from_utf8(line.to_vec()).unwrap();

            // Remove the last two bytes of the string (corresponding to \r\n)
            res.truncate(res.len()-2);

            Ok(Some(Request::Message(res)))
        } else {
            Ok(None)
        }
    }
}

/// Implement encoder trait for P2P
impl Encoder<Response> for P2PCodec {
    type Error = io::Error;

    fn encode(&mut self, msg: Response, _dst: &mut BytesMut) -> Result<(), Self::Error> {
        println!("Encoding {:?}", msg);

        Ok(())
    }
}