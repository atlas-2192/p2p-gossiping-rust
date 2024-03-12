use std::collections::HashSet;
use std::io;
use std::io::Error;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use actix::io::{FramedWrite, WriteHandler};
use actix_rt::spawn;
use actix::prelude::*;
use tokio::io::{AsyncWriteExt, split, WriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::FramedRead;
//use tokio_io::_tokio_codec::FramedRead;
use tracing::{debug, info};
use tracing::field::debug;
use tracing::log::error;
use uuid::Uuid;
use crate::codec::{RemoteToLocalCodec, Peers, PeersRequest, Message, RequestFromRemote, ResponseFromRemote, LocalToRemoteCodec, RequestToRemote, ResponseToRemote};

//use crate::remote_peer::RemotePeer;

pub struct Peer {
    socket_addr: SocketAddr,
    period: Duration,
    connect_to: Option<SocketAddr>,
    connections: scc::HashSet<Connection>,
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
            connections: Default::default()
        }
    }

    // set up peer to start listen incoming connections
    pub fn start_listening(&self, peer_addr: Addr<Peer>) {
        let addr = &self.socket_addr;
        let addr = addr.clone();

        spawn(async move {
            let listener = TcpListener::bind(addr).await.unwrap();
            while let Ok((stream, addr)) = listener.accept().await {
                debug!("peer [{addr}] connected");
                let peer = peer_addr.clone();
                RemoteToLocalConnection::create(|ctx| {
                    let (r, w) = split(stream);
                    // reading from remote connection
                    RemoteToLocalConnection::add_stream(FramedRead::new(r, RemoteToLocalCodec), ctx);
                    // writing to remote connection
                    RemoteToLocalConnection::new(addr, peer, FramedWrite::new(w, RemoteToLocalCodec, ctx))
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
            let initial_peer = LocalToRemoteConnection::create(|ctx| {
                LocalToRemoteConnection::add_stream(FramedRead::new(r, LocalToRemoteCodec), ctx);
                LocalToRemoteConnection::new(
                    socket_addr,
                    peer_ctx_address,
                    FramedWrite::new(w, LocalToRemoteCodec, ctx))
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
                let req = initial_peer_addr.try_send(RequestToRemote::PeersRequest(PeersRequest));
                info!("req: {:?}", req);
            }));

        // start sending messages with specified [`period`]
        let period = self.period.clone();
        // TODO check if removed peers are being removed while loop processing 
        // + is it ok to clone() sender in Addr<> ?
        let peers = self.connections.clone();
        let socket_addr = self.socket_addr.clone();
        peer_ctx.spawn(async {
            let msg = gen_rnd_msg();
            loop {
                tokio::time::sleep(period).await;
                peers.scan(|conn| {
                    let _ = conn.addr.send(RequestFromRemote::RandomMessage(Message{msg: msg.clone(), from: socket_addr }));
                });
            }
        }
            .into_actor(self));
    }
}

// pub enum Connection {
//     RemoteToLocalConnection(RemoteToLocalConnection),
//     LocalToRemoteConnection(LocalToRemoteConnection),
// }
#[derive(Debug, Hash, Eq, PartialEq, Clone)]
struct Connection {
    id: Uuid,
    peer_addr: SocketAddr,
    addr: Addr<Peer>,
}

/// Connection with remote peer initiated by remote peer
pub struct RemoteToLocalConnection {
    /// remote peer [`SocketAddr`]
    remote_peer_addr: SocketAddr,
    /// peer [`Actor`] address
    peer: Addr<Peer>,
    /// stream to write messages to remote peer
    write: FramedWrite<RequestFromRemote, WriteHalf<TcpStream>, RemoteToLocalCodec>,
}

impl RemoteToLocalConnection {
    pub fn new(
        peer_addr: SocketAddr,
        peer: Addr<Peer>,
        write: FramedWrite<RequestFromRemote, WriteHalf<TcpStream>, RemoteToLocalCodec>) -> Self {
        Self { remote_peer_addr: peer_addr, peer, write }
    }
}

impl Actor for RemoteToLocalConnection {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        todo!()
    }
}

impl StreamHandler<Result<ResponseFromRemote, io::Error>> for RemoteToLocalConnection {
    fn handle(&mut self, item: Result<ResponseFromRemote, io::Error>, ctx: &mut Self::Context) {
        match item {
            Ok(resp) => {
                match resp {
                    ResponseFromRemote::Peers(peers) => {

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

/// Connection with remote peer initiated by local peer
pub struct LocalToRemoteConnection {
    /// remote peer [`SocketAddr`]
    peer_addr: SocketAddr,
    /// remote peer [`Actor`] address
    remote_peer: Addr<Peer>,
    /// stream to write messages to remote peer
    write: FramedWrite<RequestToRemote, WriteHalf<TcpStream>, LocalToRemoteCodec>,
}

impl LocalToRemoteConnection {
    pub fn new(
        peer_addr: SocketAddr,
        peer: Addr<Peer>,
        write: FramedWrite<RequestToRemote, WriteHalf<TcpStream>, LocalToRemoteCodec>) -> Self {
        Self { peer_addr, remote_peer: peer, write }
    }
}

impl Actor for LocalToRemoteConnection {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        todo!()
    }
}

impl Handler<RequestFromRemote> for RemoteToLocalConnection {
    type Result = ();

    fn handle(&mut self, msg: RequestFromRemote, _ctx: &mut Self::Context) -> Self::Result {
        debug!("Handler<Request> for Connection");
        match msg {
            RequestFromRemote::RandomMessage(req) => {
                info!("sending message [{}] to [{}]", &req.msg, self.remote_peer_addr);
                self.write.write(RequestFromRemote::RandomMessage(req))
            },
            // TODO matching
            RequestFromRemote::PeersRequest(req) => {
                debug!("msg PeersRequest has sent to initial peer");
                self.write.write(RequestFromRemote::PeersRequest(req))
            },
        }
    }
}

impl StreamHandler<Result<ResponseToRemote, io::Error>> for LocalToRemoteConnection {

    fn handle(&mut self, item: Result<ResponseToRemote, io::Error>, ctx: &mut Self::Context) {
        todo!()
    }
}

impl Handler<RequestToRemote> for LocalToRemoteConnection {
    type Result = ();

    fn handle(&mut self, msg: RequestToRemote, ctx: &mut Self::Context) -> Self::Result {
        todo!()
    }
}

impl WriteHandler<std::io::Error> for LocalToRemoteConnection {}

impl WriteHandler<std::io::Error> for RemoteToLocalConnection {}


fn gen_rnd_msg() -> String {
    use random_word::Lang;
    let msg = random_word::gen(Lang::En);
    String::from(msg)
}

#[derive(Debug, Message)]
#[rtype(result = "()")]
enum AddPeer {
   Local(Addr<LocalToRemoteConnection>),
   Remote(Addr<RemoteToLocalConnection>),
}


 impl Handler<AddPeer> for Peer {
    type Result = ();

    fn handle(&mut self, msg: AddPeer, _ctx: &mut Self::Context) -> Self::Result {
        match msg {
            AddPeer::Local(addr) => {self.connections.}
            AddPeer::Remote(addr) => {}
        }
    }
}