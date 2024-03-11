use std::collections::HashSet;
use std::io;
use std::io::Error;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
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
use crate::codec::{PeerConnectionCodec, Peers, PeersRequest, Message, Request, Response};
use crate::codec::Request::RandomMessage;
//use crate::remote_peer::RemotePeer;

pub struct Peer {
    socket_addr: SocketAddr,
    period: Duration,
    connect_to: Option<SocketAddr>,
    peers: scc::HashSet<Addr<P2PConnection>>,
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
                P2PConnection::create(|ctx| {
                    let (r, w) = split(stream);
                    P2PConnection::add_stream(FramedRead::new(r, PeerConnectionCodec), ctx);
                    P2PConnection::new(addr, peer, actix::io::FramedWrite::new(w, PeerConnectionCodec, ctx))
                });
            }
        });
    }
}

impl Actor for Peer {
    type Context = Context<Self>;

    fn started(&mut self, peer_ctx: &mut Self::Context) {
        // Peer is the first in the network
        if self.connect_to.is_none() {
            info!("Peer has started on [{}]. Waiting for incoming connections", self.socket_addr);
            self.start_listening(peer_ctx.address());
            return;
        }
        // Trying to connect initial peer
        self.start_listening(peer_ctx.address());
        let connect_to = self.connect_to.unwrap();
        let peer_ctx_address = peer_ctx.address().clone();
        info!("Peer has started on [{}]. Trying to connect [{connect_to}]", self.socket_addr);
        peer_ctx.wait(async move {
            // ERRORS
            let stream = TcpStream::connect(connect_to).await.unwrap();

            let socket_addr = stream.peer_addr().unwrap();
            let (r, w) = split(stream);
            let initial_peer = P2PConnection::create(|ctx| {
                P2PConnection::add_stream(FramedRead::new(r, PeerConnectionCodec), ctx);
                P2PConnection::new(
                    socket_addr,
                    peer_ctx_address,
                    actix::io::FramedWrite::new(w, PeerConnectionCodec, ctx))
            });
            Ok(initial_peer)
        }
            .into_actor(self)
            .map_err(move |e: Box<dyn std::error::Error + Send>, _act, ctx| {
                error!("Couldn't establish connection with peer: {connect_to}, error {e}");
                ctx.stop();
            })
            .map(|res, _actor, _ctx| {
                let initial_peer_addr = res.unwrap();
                // request all the other peers
                let req = initial_peer_addr.try_send(Request::PeersRequest(PeersRequest));
                info!("req: {:?}", req);
            }));

        // start sending messages with specified [`period`]
        let period = &self.period;
        let period = period.clone();
        let peers = &self.peers;
        let peers = peers.clone();
        let socket_addr = &self.socket_addr;
        let socket_addr = socket_addr.clone();
        peer_ctx.spawn(async move {
            let msg = gen_rnd_msg();
            loop {
                tokio::time::sleep(period.clone()).await;
                peers.scan(|addr| {
                    let _ = addr.send(RandomMessage(Message{msg: msg.clone(), from: socket_addr }));
                });
            }
        }
            .into_actor(self));
    }
}

/// Connection with remote peer
pub struct P2PConnection {
    /// remote peer [`SocketAddr`]
    peer_addr: SocketAddr,
    /// remote peer [`Actor`] address
    peer: Addr<Peer>,
    /// stream to write messages to remote peer
    write: actix::io::FramedWrite<Request, WriteHalf<TcpStream>, PeerConnectionCodec>,
}

impl WriteHandler<std::io::Error> for P2PConnection {}

impl WriteHandler<std::io::Error> for Peer {}

impl P2PConnection {
    pub fn new(
        peer_addr: SocketAddr,
        peer: Addr<Peer>,
        write: actix::io::FramedWrite<Request, WriteHalf<TcpStream>, PeerConnectionCodec>) -> Self {
        Self { peer_addr, peer, write }
    }
}

impl Actor for P2PConnection {
    type Context = Context<Self>;
}

impl StreamHandler<Result<Response, io::Error>> for P2PConnection {
    fn handle(&mut self, item: Result<Response, io::Error>, ctx: &mut Self::Context) {
        match item {
            Ok(resp) => {
                match resp {
                    Response::Peers(peers) => {
                        let _ = self.peer.try_send(Request::PeersRequest(PeersRequest));
                                    // .into_actor(self)
                                    // .map_err(|err, _act, ctx| {
                                    //     // TODO handle possible errors
                                    //     error!("Error: {err}");
                                    //     ctx.stop();
                                    // });
                    }
                    _ => {debug!("ERROR wrong handler!"); panic!()}
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
            Request::RandomMessage(req) => {
                info!("sending message [{}] to [{}]", &req.msg, self.peer_addr);
                self.write.write(Request::RandomMessage(req))
            },
            // TODO matching
            Request::PeersRequest(req) => {
                debug!("msg PeersRequest has sent to initial peer");
                self.write.write(Request::PeersRequest(req))
            },
        }
    }
}

impl Handler<Request> for Peer {
    type Result = ();

    fn handle(&mut self, msg: Request, _ctx: &mut Self::Context) -> Self::Result {
        debug!("Handler<Peers> for Peer");
        match msg {
            Request::RandomMessage(msg) => {
                info!("Received message [{}] from [{}]", msg.msg, msg.from);
            }
            Request::PeersRequest(req) => {
                debug!("PeersRequest has been received");
                //let response = Peers()
            }
        }
    }
}

fn gen_rnd_msg() -> String {
    use random_word::Lang;
    let msg = random_word::gen(Lang::En);
    String::from(msg)
}