use std::collections::HashSet;
use std::net::SocketAddr;
use actix::Message;

/*
    Two types of messages are needed as
    there are two types of connections
 */

#[derive(Debug, Message)]
#[rtype(result = "()")]
pub enum OutMessage {
    Request(Request),
    Response(Response),
}

#[derive(Debug, Message)]
#[rtype(result = "()")]
pub enum InMessage {
    Request(Request),
    Response(Response),
}

#[derive(Debug, Message)]
#[rtype(result = "()")]
pub enum Response {
    PeersResponse(HashSet<SocketAddr>),
    // TODO
    // MessageResponse(String, SocketAddr),
    /// Result of handshake is socket address of peer answering to request
    AcceptHandshake(bool),
}

#[derive(Debug, Message)]
#[rtype(result = "()")]
pub enum Request {
    /// (random message, sender)
    MessageRequest(String, SocketAddr),
    /// request for all active peers in network.
    PeersRequest,
    /// Send peer's listening address to remote peer in order
    /// to be able to be discovered by other peers in network
    TryHandshake {
        /// sender listening address
        sender: SocketAddr,
        /// send request to
        receiver: SocketAddr
    },
}

pub mod actor {
    use std::collections::HashSet;
    use std::net::SocketAddr;
    use actix::Message;

    /// requests between connection actor and peer actor
    #[derive(Debug, Message)]
    #[rtype(result = "()")]
    pub enum ActorRequest {
        Message(String, SocketAddr),
        PeersRequest,
    }

    /// responses between connection actor and peer actor
    #[derive(Debug, Message)]
    #[rtype(result = "()")]
    pub enum ActorResponse {
        Peers(HashSet<SocketAddr>),
        Empty,
    }
}