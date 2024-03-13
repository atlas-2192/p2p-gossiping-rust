use std::collections::HashSet;
use std::net::SocketAddr;
use actix::Message;

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
    MessageResponse(String, SocketAddr),
}

#[derive(Debug, Message)]
#[rtype(result = "()")]
pub enum Request {
    /// (random message, sender)
    MessageRequest(String, SocketAddr),
    /// request for all active peers in network
    PeersRequest,
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