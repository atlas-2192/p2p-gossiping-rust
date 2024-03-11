use std::collections::HashSet;
use std::io;
use std::io::Error;
use std::net::SocketAddr;
use std::str::FromStr;
use std::time::Duration;
use actix::{Actor, ActorContext, ActorFutureExt, ActorTryFutureExt, Addr, AsyncContext, Context, Handler, StreamHandler, WrapFuture};
use actix::io::{ WriteHandler};
use actix_rt::spawn;
use tokio::io::{AsyncWriteExt, split, WriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::FramedRead;
//use tokio_io::_tokio_codec::FramedRead;
use tracing::{debug, info};
use tracing::field::debug;
use tracing::log::error;
use crate::codec::{PeerConnectionCodec, Peers, PeersRequest, RemotePeerConnectionCodec, Request, Response};
use crate::remote_peer::RemotePeer;

pub struct Peer {
    socket_addr: SocketAddr,
    period: Duration,
    connect_to: Option<SocketAddr>,
    peers: HashSet<Addr<RemotePeer>>,
}

impl Peer {
    pub fn new(port: u32, period: Duration, connect_to: Option<SocketAddr>) -> Self {
        let socket_addr = format!("127.0.0.1:{}", port);
        let socket_addr = SocketAddr::from_str(&socket_addr).unwrap();

        Self {
            socket_addr,
            period,
            connect_to,
            peers: Default::default()
        }
    }

    pub fn start_listening(&self, self_addr: Addr<Peer>) {
        let addr = &self.socket_addr;
        let addr = addr.clone();

        spawn(async move {
            let listener = TcpListener::bind(addr).await.unwrap();
            while let Ok((stream, addr)) = listener.accept().await {
                debug!("peer [{addr}] connected");
                let peer = self_addr.clone();
                Connection::create(|ctx| {
                    let (r, w) = split(stream);
                    Connection::add_stream(FramedRead::new(r, RemotePeerConnectionCodec), ctx);
                    Connection::new(peer, actix::io::FramedWrite::new(w, PeerConnectionCodec, ctx))
                });
            }
        });
    }
}

impl Actor for Peer {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        // Peer is the first in the network
        if self.connect_to.is_none() {
            info!("Peer has started on [{}]. Waiting for incoming connections", self.socket_addr);
            self.start_listening(ctx.address());
            return;
        }
        // Trying to connect initial peer
        self.start_listening(ctx.address());
        let connect_to = self.connect_to.unwrap();
        info!("Peer has started on [{}]. Trying to connect [{connect_to}]", self.socket_addr);
        ctx.wait(async move {
            // ERRORS
            let stream = TcpStream::connect(connect_to).await.unwrap();

            let addr = stream.peer_addr().unwrap();
            let (r, w) = split(stream);
            let initial_peer = RemotePeer::create(|ctx| {
                RemotePeer::add_stream(FramedRead::new(r, RemotePeerConnectionCodec), ctx);
                RemotePeer::new(addr, actix::io::FramedWrite::new(w, PeerConnectionCodec, ctx))
            });
            Ok(initial_peer)
        }
            .into_actor(self)
            .map_err(|e, _act, ctx| {
                error!("Couldn't establish connection with peer: {connect_to}, error {e}");
                ctx.stop();
            })
            .map(|res, actor, _ctx| {
                let initial_peer = res.unwrap();
                // request all the other peers
                let req = initial_peer.send(Request::PeersRequest(PeersRequest));
                debug!("msg PeersRequest has sent to initial peer");
                self.peers.insert(initial_peer);
            }));

        // TODO start sending messages
        ctx.spawn(async move {  });
    }
}

/// Connection with remote peer
pub struct Connection {
    /// remote peer [`Actor`] address
    peer: Addr<Peer>,
    /// stream to write messages to remote peer
    write: actix::io::FramedWrite<Request, WriteHalf<TcpStream>, PeerConnectionCodec>,
}

impl WriteHandler<std::io::Error> for Connection {}

impl WriteHandler<std::io::Error> for Peer {}

impl Connection {
    pub fn new(
        peer: Addr<Peer>,
        write: actix::io::FramedWrite<Request, WriteHalf<TcpStream>, PeerConnectionCodec>) -> Self {
        Self { peer, write }
    }
}

impl Actor for Connection {
    type Context = Context<Self>;
}

impl Handler<PeersRequest> for Connection {
    type Result = Result<Peers, io::Error>;

    fn handle(&mut self, msg: PeersRequest, ctx: &mut Self::Context) -> Self::Result {
        self.write.write(Request::PeersRequest(PeersRequest));
        unimplemented!()
    }
}

impl StreamHandler<Result<Response, io::Error>> for Connection {
    fn handle(&mut self, item: Result<Response, io::Error>, ctx: &mut Self::Context) {
        match item {
            Ok(resp) => {
                match resp {
                    Response::Peers(peers) => {
                        let _ = self.peer.send(Peers(peers.0))
                                    .into_actor(self)
                                    .map_err(|err, _act, ctx| {
                                        // TODO handle possible errors
                                        error!("Error: {err}");
                                        ctx.stop();
                                    });
                    }
                }
            }
            Err(err) => {
                error!("Error while processing response: {err}");
                ctx.stop();
            }
        }
    }
}

impl Handler<Request> for Connection {
    type Result = ();

    fn handle(&mut self, msg: Request, _ctx: &mut Self::Context) -> Self::Result {
        debug!("Handler<Request> for Connection");
        match msg {
            Request::RandomMessage(msg) => self.write.write(Request::RandomMessage(msg)),
            // TODO matching
            Request::PeersRequest(req) => self.write.write(Request::PeersRequest(req)),
        }
    }
}

impl Handler<Peers> for Peer {
    type Result = ();

    fn handle(&mut self, msg: Peers, _ctx: &mut Self::Context) -> Self::Result {
        debug!("Handler<Peers> for Peer");
        let mut remote_peers = msg.0;
        // In order not to send msg to itself
        remote_peers.remove(&self.socket_addr);
        // Including every peer in our network
        for socket_addr in remote_peers.iter() {
            let remote_peer = RemotePeer { socket_addr: *socket_addr }.start();
            self.peers.insert(remote_peer);
        }
    }
}