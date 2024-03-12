use std::hash::{Hash, Hasher};
use std::io;
use std::net::SocketAddr;
use actix::{Actor, ActorContext, Addr, AsyncContext, Context, Handler, StreamHandler};
use actix::io::{FramedWrite, WriteHandler};
use tokio::io::WriteHalf;
use tokio::net::TcpStream;
use tracing::{debug, info};
use tracing::log::error;
use crate::codec::{ToRemoteCodec, RequestToRemote, ResponseFromRemote, FromRemoteCodec};
use crate::peer::Peer;

/// Connection initiated by remote peer
pub struct FromRemoteConnection {
    /// remote peer [`SocketAddr`]
    peer_addr: SocketAddr,
    /// remote peer [`Actor`] address
    peer_actor: Addr<Peer>,
    /// stream to write messages to remote peer
    write: FramedWrite<RequestToRemote, WriteHalf<TcpStream>, ToRemoteCodec>,
}

impl Hash for FromRemoteConnection {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.peer_addr.hash(state);
    }
}

impl PartialEq for FromRemoteConnection {
    fn eq(&self, other: &Self) -> bool {
        self.peer_addr == other.peer_addr
    }
}

impl Eq for FromRemoteConnection {}

impl FromRemoteConnection {
    pub fn new(
        peer_addr: SocketAddr,
        peer: Addr<Peer>,
        write: FramedWrite<RequestToRemote, WriteHalf<TcpStream>, ToRemoteCodec>) -> Self {
        Self { peer_addr, peer_actor: peer, write }
    }
}

impl Actor for FromRemoteConnection {
    type Context = Context<Self>;

    // Notify peer actor about creation of new connection actor
    fn started(&mut self, ctx: &mut Self::Context) {
        let addr = ctx.address();
        let added = self.peer_actor.try_send(crate::peer::AddFromRemoteConnection(addr));
    }
}

impl StreamHandler<Result<ResponseFromRemote, io::Error>> for FromRemoteConnection {
    fn handle(&mut self, item: Result<ResponseFromRemote, io::Error>, ctx: &mut Self::Context) {
        match item {
            Ok(resp) => {
                match resp {
                    ResponseFromRemote::Peers(peers) => {
                        let sent = self.peer_actor.try_send(crate::peer::AddPeers(peers));
                        debug!("peers sent from conn to peer : {:?}", sent);
                    }
                    ResponseFromRemote::Empty => { debug!("got empty response"); }
                }
            }
            Err(err) => {
                error!("Error while processing response: {err}");
                ctx.stop();
            }
        }
    }
}

impl Handler<RequestToRemote> for FromRemoteConnection {
    type Result = ();

    fn handle(&mut self, msg: RequestToRemote, _ctx: &mut Self::Context) -> Self::Result {
        debug!("Handler<Request> for Connection");
        match msg {
            RequestToRemote::RandomMessage(msg, addr) => {
                info!("sending message [{}] to [{}]", &msg, self.peer_addr);
                self.write.write(RequestToRemote::RandomMessage(msg, addr))
            },
            // TODO matching
            RequestToRemote::PeersRequest => {
                debug!("msg PeersRequest has sent to initial peer");
                self.write.write(RequestToRemote::PeersRequest)
            },
        }
    }
}

impl WriteHandler<std::io::Error> for FromRemoteConnection {}

/// Connection initiated by current peer
pub struct ToRemoteConnection {
    /// remote peer [`SocketAddr`]
    peer_addr: SocketAddr,
    /// remote peer [`Actor`] address
    peer_actor: Addr<Peer>,
    /// stream to write messages to remote peer
    write: FramedWrite<RequestToRemote, WriteHalf<TcpStream>, FromRemoteCodec>,
}

impl Hash for ToRemoteConnection {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.peer_addr.hash(state);
    }
}

impl PartialEq for ToRemoteConnection {
    fn eq(&self, other: &Self) -> bool {
        self.peer_addr == other.peer_addr
    }
}

impl Eq for ToRemoteConnection {}

impl ToRemoteConnection {
    pub fn new(
        peer_addr: SocketAddr,
        peer: Addr<Peer>,
        write: FramedWrite<RequestToRemote, WriteHalf<TcpStream>, FromRemoteCodec>) -> Self {
        Self { peer_addr, peer_actor: peer, write }
    }
}

impl Actor for ToRemoteConnection {
    type Context = Context<Self>;

    // Notify peer actor about creation of new connection actor
    fn started(&mut self, ctx: &mut Self::Context) {
        let addr = ctx.address();
        let added = self.peer_actor.try_send(crate::peer::AddToRemoteConnection(addr));
    }
}

impl StreamHandler<Result<ResponseFromRemote, io::Error>> for ToRemoteConnection {
    fn handle(&mut self, item: Result<ResponseFromRemote, io::Error>, ctx: &mut Self::Context) {
        match item {
            Ok(resp) => {
                match resp {
                    ResponseFromRemote::Peers(peers) => {
                        let sent = self.peer_actor.try_send(crate::peer::AddPeers(peers));
                        debug!("peers sent from conn to peer : {:?}", sent);
                    }
                    ResponseFromRemote::Empty => { debug!("got empty response"); }
                }
            }
            Err(err) => {
                error!("Error while processing response: {err}");
                ctx.stop();
            }
        }
    }
}

impl Handler<RequestToRemote> for ToRemoteConnection {
    type Result = ();

    fn handle(&mut self, msg: RequestToRemote, _ctx: &mut Self::Context) -> Self::Result {
        debug!("Handler<Request> for Connection");
        match msg {
            RequestToRemote::RandomMessage(msg, addr) => {
                info!("sending message [{}] to [{}]", &msg, self.peer_addr);
                self.write.write(RequestToRemote::RandomMessage(msg, addr))
            },
            // TODO matching
            RequestToRemote::PeersRequest => {
                debug!("msg PeersRequest has sent to initial peer");
                self.write.write(RequestToRemote::PeersRequest)
            },
        }
    }
}

impl WriteHandler<std::io::Error> for ToRemoteConnection {}