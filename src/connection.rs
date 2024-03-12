use std::hash::{Hash, Hasher};
use std::io;
use std::net::SocketAddr;
use actix::{Actor, ActorContext, Addr, AsyncContext, Context, Handler, StreamHandler};
use actix::io::{FramedWrite, WriteHandler};
use tokio::io::WriteHalf;
use tokio::net::TcpStream;
use tracing::{debug, info};
use tracing::log::error;
use crate::codec::{P2PCodec, Request, Response};
use crate::peer::Peer;

/// Connection with remote peer
pub struct P2PConnection {
    /// remote peer [`SocketAddr`]
    peer_addr: SocketAddr,
    /// remote peer [`Actor`] address
    peer_actor: Addr<Peer>,
    /// stream to write messages to remote peer
    write: FramedWrite<Request, WriteHalf<TcpStream>, P2PCodec>,
}

impl Hash for P2PConnection {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.peer_addr.hash(state);
    }
}

impl PartialEq for P2PConnection {
    fn eq(&self, other: &Self) -> bool {
        self.peer_addr == other.peer_addr
    }
}

impl Eq for P2PConnection {}

impl P2PConnection {
    pub fn new(
        peer_addr: SocketAddr,
        peer: Addr<Peer>,
        write: FramedWrite<Request, WriteHalf<TcpStream>, P2PCodec>) -> Self {
        Self { peer_addr, peer_actor: peer, write }
    }
}

impl Actor for P2PConnection {
    type Context = Context<Self>;

    // Notify peer actor about creation of new connection actor
    fn started(&mut self, ctx: &mut Self::Context) {
        let addr = ctx.address();
        let added = self.peer_actor.try_send(crate::peer::AddConnection(addr));
    }
}

impl StreamHandler<Result<Response, io::Error>> for P2PConnection {
    fn handle(&mut self, item: Result<Response, io::Error>, ctx: &mut Self::Context) {
        match item {
            Ok(resp) => {
                match resp {
                    Response::Peers(peers) => {
                        let sent = self.peer_actor.try_send(crate::peer::AddPeers(peers));
                        debug!("peers sent from conn to peer : {:?}", sent);
                    }
                    Response::Empty => { debug!("got empty response"); }
                }
            }
            Err(err) => {
                error!("Error while processing response: {err}");
                ctx.stop();
            }
        }
    }
}

impl Handler<Request> for P2PConnection {
    type Result = ();

    fn handle(&mut self, msg: Request, _ctx: &mut Self::Context) -> Self::Result {
        debug!("Handler<Request> for Connection");
        match msg {
            Request::RandomMessage(msg, addr) => {
                info!("sending message [{}] to [{}]", &msg, self.peer_addr);
                self.write.write(Request::RandomMessage(msg, addr))
            },
            // TODO matching
            Request::PeersRequest => {
                debug!("msg PeersRequest has sent to initial peer");
                self.write.write(Request::PeersRequest)
            },
        }
    }
}

impl WriteHandler<std::io::Error> for P2PConnection {}