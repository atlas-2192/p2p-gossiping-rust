use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::io;
use std::io::Error;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use actix::dev::MessageResponse;
use actix::io::{FramedWrite, WriteHandler};
use actix_rt::spawn;
use actix::prelude::*;
use tokio::io::{AsyncWriteExt, split, WriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::FramedRead;
//use tokio_io::_tokio_codec::FramedRead;
use tracing::{debug, info, warn};
use tracing::field::debug;
use tracing::log::error;
use uuid::Uuid;
use crate::codec::{Request, Response, P2PCodec};
use crate::connection::P2PConnection;

pub struct Peer {
    /// address being listened by peer
    socket_addr: SocketAddr,
    /// messaging period in which messages to peers are sent
    period: Duration,
    /// initial peer to connect and get [`SocketAddr`] of all the peers in network
    connect_to: Option<SocketAddr>,
    /// peers to connect and to respond other peers
    peers: HashSet<SocketAddr>,
    /// established connections (actors to send messages)
    connections: HashSet<Addr<P2PConnection>>,
}

/// Peer running on the current process or host
impl Peer {
    pub fn new(port: u32, period: Duration, connect_to: Option<SocketAddr>) -> Self {
        let socket_addr = format!("127.0.0.1:{}", port);
        let socket_addr = SocketAddr::from_str(&socket_addr).unwrap();

        Self {
            socket_addr,
            period,
            connect_to,
            peers: Default::default(),
            connections: Default::default()
        }
    }

    // set up peer to start listen incoming connections
    fn start_listening(&self, peer_addr: Addr<Peer>) {
        let addr = self.socket_addr.clone();
        spawn(async move {
            let listener = TcpListener::bind(addr).await.unwrap();
            while let Ok((stream, addr)) = listener.accept().await {
                debug!("peer [{addr}] connected");
                let peer = peer_addr.clone();
                P2PConnection::create(|ctx| {
                    let (r, w) = split(stream);
                    // reading from remote peer connection
                    P2PConnection::add_stream(FramedRead::new(r, P2PCodec), ctx);
                    // writing to remote peer connection
                    P2PConnection::new(addr, peer, FramedWrite::new(w, P2PCodec, ctx))
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
            TcpStream::connect(connect_to).await
        }
            .into_actor(self)
            .map_err(move |e, _act, ctx| {
                error!("Couldn't establish connection with peer: {connect_to}, error {e}");
                ctx.stop();
            })
            .map(|res, _actor, _ctx| {
                let stream = res.unwrap();
                let socket_addr = stream.peer_addr().unwrap();
                let (r, w) = split(stream);
                let initial_peer = P2PConnection::create(|ctx| {
                    P2PConnection::add_stream(FramedRead::new(r, P2PCodec), ctx);
                    P2PConnection::new(
                        socket_addr,
                        peer_ctx_address,
                        FramedWrite::new(w, P2PCodec, ctx))
                });

                //Ok(initial_peer)

                // request all the other peers
                let res = initial_peer.try_send(Request::PeersRequest);
                debug!("res: {:?}", res);
            })
        );

        debug!("is waiting : {}", peer_ctx.waiting());
        debug!("peer ctx state : {:?}", peer_ctx.state());

        // start sending messages with specified [`period`]
        let period = self.period.clone();
        // TODO check if removed peers are being removed while loop processing
        // + is it ok to clone() sender in Addr<> ?
        //let peers = self.connections.clone();
        let socket_addr = self.socket_addr.clone();
        // peer_ctx.spawn(async {
        //     let msg = gen_rnd_msg();
        //     loop {
        //         //tokio::time::sleep(period).await;
        //         // peers.scan(|conn| {
        //         //     let _ = conn.peer.send(Request::RandomMessage(Message{msg: msg.clone(), from: socket_addr }));
        //         // });
        //     }
        // }
        //     .into_actor(self));
    }

    fn stopped(&mut self, ctx: &mut Self::Context) {
        todo!()
    }
}

#[derive(Debug, Message)]
#[rtype(result = "()")]
pub(crate) struct AddConnection(pub(crate) Addr<P2PConnection>);

impl Handler<AddConnection> for Peer {
    type Result = ();

    fn handle(&mut self, msg: AddConnection, _ctx: &mut Self::Context) -> Self::Result {
        debug!("connection {:?} added to peer's connections", &msg.0);
        let _ = self.connections.insert(msg.0);
    }
}

#[derive(Debug, Message)]
#[rtype(result = "()")]
pub(crate) struct AddPeers(pub(crate) HashSet<SocketAddr>);

impl Handler<AddPeers> for Peer {
    type Result = ();

    fn handle(&mut self, msg: AddPeers, ctx: &mut Self::Context) -> Self::Result {
        let mut peers_to_add = msg.0;
        // we don't need to store our own peer address here
        peers_to_add.remove(&self.socket_addr);
        self.peers.extend(peers_to_add);
        let peers_to_connect = self.peers.clone();
        debug!("peers hash set: {:?}", &self.peers);
        // establish connection with required peers
        ctx.notify(ConnectPeers(peers_to_connect));
    }
}

/// notify ourself than we need to establish connection with peers
#[derive(Debug, Message)]
#[rtype(result = "()")]
struct ConnectPeers(HashSet<SocketAddr>);

impl Handler<ConnectPeers> for Peer {
    type Result = ();

    fn handle(&mut self, msg: ConnectPeers, peer_ctx: &mut Self::Context) -> Self::Result {
        let peer_addr = peer_ctx.address();
        let peers = Arc::new(msg.0);
        peer_ctx.spawn(async move {
            // TODO define retry count
            let mut errors = HashSet::new();
            for peer in peers.iter() {
                let stream = TcpStream::connect(peer).await;
                if let Ok(stream) = stream {
                    P2PConnection::create(|ctx| {
                        let (r, w) = split(stream);
                        P2PConnection::add_stream(FramedRead::new(r, P2PCodec), ctx);
                        P2PConnection::new(*peer, peer_addr.clone(), FramedWrite::new(w, P2PCodec, ctx))
                    });
                } else {
                    warn!("couldn't establish connection with peer: {peer}");
                    errors.insert(*peer);
                }
            }
            errors
        }
            .into_actor(self)
            .map(|errors, _actor, ctx| {
                // Retry establishing connection later TODO wait a few seconds before this action
                debug!("retrying to establish connection with peers: {:?} after 3 seconds", errors);
                ctx.notify_later(ConnectPeers(errors), Duration::from_secs(3));
            }));
    }
}

#[derive(Debug, Message)]
#[rtype(result = "PeersResponse")]
struct GetPeers;

struct PeersResponse(HashSet<SocketAddr>);

impl Handler<GetPeers> for Peer {
    type Result = MessageResult<GetPeers>;

    fn handle(&mut self, _msg: GetPeers, _ctx: &mut Self::Context) -> Self::Result {
        MessageResult(PeersResponse(self.peers.clone()))
    }
}

#[derive(Message)]
#[rtype(result = "()")]
struct AddStreamToConnection(P2PConnection);

impl Handler<AddStreamToConnection> for P2PConnection {
    type Result = ();

    fn handle(&mut self, msg: AddStreamToConnection, ctx: &mut Self::Context) -> Self::Result {
        let conn = msg.0;
        conn.start();
    }
}


fn gen_rnd_msg() -> String {
    use random_word::Lang;
    let msg = random_word::gen(Lang::En);
    String::from(msg)
}