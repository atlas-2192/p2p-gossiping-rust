use std::hash::{Hash, Hasher};
use std::io;
use std::net::SocketAddr;
use actix::{Actor, ActorContext, Addr, AsyncContext, Context, Handler, StreamHandler};
use actix::io::{FramedWrite, WriteHandler};
use tokio::io::WriteHalf;
use tokio::net::TcpStream;
use tracing::{debug, info};
use tracing::log::error;
use crate::codec::{OutCodec, OutgoingRequest, ResponseToOutgoing, InCodec, ResponseToIncoming, IncomingRequest};
use crate::peer::Peer;

/// Connection initiated by remote peer
pub struct IncomingConnection {
    /// remote peer [`SocketAddr`]
    peer_addr: SocketAddr,
    /// remote peer [`Actor`] address
    peer_actor: Addr<Peer>,
    /// stream to write messages to remote peer
    write: FramedWrite<IncomingRequest, WriteHalf<TcpStream>, InCodec>,
}

impl Hash for IncomingConnection {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.peer_addr.hash(state);
    }
}

impl PartialEq for IncomingConnection {
    fn eq(&self, other: &Self) -> bool {
        self.peer_addr == other.peer_addr
    }
}

impl Eq for IncomingConnection {}

impl IncomingConnection {
    pub fn new(
        peer_addr: SocketAddr,
        peer: Addr<Peer>,
        write: FramedWrite<IncomingRequest, WriteHalf<TcpStream>, InCodec>) -> Self {
        Self { peer_addr, peer_actor: peer, write }
    }
}

impl Actor for IncomingConnection {
    type Context = Context<Self>;

    // Notify peer actor about creation of new connection actor
    fn started(&mut self, ctx: &mut Self::Context) {
        let addr = ctx.address();
        let added = self.peer_actor.try_send(crate::peer::AddFromRemoteConnection(addr));
    }
}

impl StreamHandler<Result<ResponseToIncoming, io::Error>> for IncomingConnection {
    fn handle(&mut self, item: Result<ResponseToIncoming, io::Error>, ctx: &mut Self::Context) {
        match item {
            Ok(resp) => {
                match resp {
                    ResponseToIncoming::Peers(peers) => {
                        let sent = self.peer_actor.try_send(crate::peer::AddPeers(peers));
                        debug!("peers sent from conn to peer : {:?}", sent);
                    }
                    ResponseToIncoming::Empty => { debug!("got empty response"); }
                }
            }
            Err(err) => {
                error!("Error while processing response: {err}");
                ctx.stop();
            }
        }
    }
}

impl Handler<IncomingRequest> for IncomingConnection {
    type Result = ();

    fn handle(&mut self, msg: IncomingRequest, _ctx: &mut Self::Context) -> Self::Result {
        debug!("Handler<Request> for Connection");
        match msg {
            IncomingRequest::RandomMessage(msg, addr) => {
                info!("sending message [{}] to [{}]", &msg, self.peer_addr);
                self.write.write(IncomingRequest::RandomMessage(msg, addr))
            },
            IncomingRequest::PeersRequest => {
                debug!("msg PeersRequest has sent to initial peer");
                self.write.write(IncomingRequest::PeersRequest)
            },
        }
    }
}

impl WriteHandler<std::io::Error> for IncomingConnection {}

/// Connection initiated by current peer
pub struct OutgoingConnection {
    /// remote peer [`SocketAddr`]
    peer_addr: SocketAddr,
    /// remote peer [`Actor`] address
    peer_actor: Addr<Peer>,
    /// stream to write messages to remote peer
    write: FramedWrite<OutgoingRequest, WriteHalf<TcpStream>, OutCodec>,
}

impl Hash for OutgoingConnection {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.peer_addr.hash(state);
    }
}

impl PartialEq for OutgoingConnection {
    fn eq(&self, other: &Self) -> bool {
        self.peer_addr == other.peer_addr
    }
}

impl Eq for OutgoingConnection {}

impl OutgoingConnection {
    pub fn new(
        peer_addr: SocketAddr,
        peer: Addr<Peer>,
        write: FramedWrite<OutgoingRequest, WriteHalf<TcpStream>, OutCodec>) -> Self {
        Self { peer_addr, peer_actor: peer, write }
    }
}

impl Actor for OutgoingConnection {
    type Context = Context<Self>;

    // Notify peer actor about creation of new connection actor
    fn started(&mut self, ctx: &mut Self::Context) {
        let addr = ctx.address();
        let added = self.peer_actor.try_send(crate::peer::AddToRemoteConnection(addr));
    }
}

impl StreamHandler<Result<ResponseToOutgoing, io::Error>> for OutgoingConnection {
    fn handle(&mut self, item: Result<ResponseToOutgoing, io::Error>, ctx: &mut Self::Context) {
        match item {
            Ok(resp) => {
                match resp {
                    ResponseToOutgoing::Peers(peers) => {
                        let sent = self.peer_actor.try_send(crate::peer::AddPeers(peers));
                        debug!("peers sent from conn to peer : {:?}", sent);
                    }
                    ResponseToOutgoing::Empty => { debug!("got empty response"); }
                }
            }
            Err(err) => {
                error!("Error while processing response: {err}");
                ctx.stop();
            }
        }
    }
}

impl Handler<OutgoingRequest> for OutgoingConnection {
    type Result = ();

    fn handle(&mut self, msg: OutgoingRequest, _ctx: &mut Self::Context) -> Self::Result {
        debug!("Handler<Request> for Connection");
        match msg {
            OutgoingRequest::RandomMessage(msg, addr) => {
                info!("sending message [{}] to [{}]", &msg, self.peer_addr);
                self.write.write(OutgoingRequest::RandomMessage(msg, addr))
            },
            OutgoingRequest::PeersRequest => {
                debug!("msg PeersRequest has sent to initial peer");
                self.write.write(OutgoingRequest::PeersRequest)
            },
        }
    }
}

impl WriteHandler<std::io::Error> for OutgoingConnection {}