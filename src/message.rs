
/// Messages being exchanged between actors and sent across the network
use std::collections::HashSet;
use std::net::SocketAddr;
use actix::Message;
use serde_derive::{Deserialize, Serialize};

/*
     Two types of messages are needed as
     there are two types of connections
*/
#[derive(Deserialize, Serialize, Debug, Message)]
#[rtype(result = "()")]
pub enum OutMessage {
    Request(Request),
    Response(Response),
}

#[derive(Deserialize, Serialize, Debug, Message)]
#[rtype(result = "()")]
pub enum InMessage {
    Request(Request),
    Response(Response),
}
#[derive(Deserialize, Serialize, Debug, Message)]
#[rtype(result = "()")]
pub enum Response {
    PeersResponse(HashSet<SocketAddr>),
    /// Result of handshake is socket address of peer answering to request
    AcceptHandshake(bool),
}
#[derive(Deserialize, Serialize, Debug, Message)]
#[rtype(result = "()")]
pub enum Request {
    /// (random message, sender)
    MessageRequest(String, SocketAddr),
    /// request for all active peers in network.
    PeersRequest,
    /// Send peer's listening address to remote peer in order
    /// to be able to be discovered by other peers in network
    TryHandshake {
        /// naive secret key emulation
        token: Vec<u8>,
        /// sender listening address
        sender: SocketAddr,
        /// send request to
        receiver: SocketAddr
    },
}
