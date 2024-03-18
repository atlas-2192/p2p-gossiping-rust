use std::collections::HashSet;
use std::hash::{Hash};
use std::net::SocketAddr;
use std::str::FromStr;
use std::io;
use std::io::{Error, ErrorKind};
use std::sync::Arc;
use std::time::Duration;
use actix::io::{FramedWrite};
use actix_rt::spawn;
use actix::prelude::*;
use tokio::io::{split};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::FramedRead;
use tracing::{debug, info, warn};
use tracing::log::error;
use crate::codec::{deserialize_data, InCodec, OutCodec, serialize_data};
use crate::connection::{InConnection, OutConnection};
use crate::message::{InMessage, OutMessage};
use crate::message::Request::{MessageRequest, TryHandshake};

/// Peer running on the current process or host
pub struct Peer {
    /// address being listened by peer
    socket_addr: SocketAddr,
    /// messaging period in which messages to peers are sent
    period: Duration,
    /// Message being sent to all the other peers in network
    message: String,
    /// initial peer to connect and get [`SocketAddr`] of all the peers in network
    connect_to: Option<SocketAddr>,
    /// peers to connect and to respond other peers
    peers: HashSet<SocketAddr>,
    /// established connections (actors to send messages)
    connections: HashSet<Connection>,
}

/// Connection between 2 peers
#[derive(Eq, Hash, PartialEq)]
pub enum Connection {
    /// Remote peer is initiator
    In(Addr<InConnection>),
    /// Current peer is initiator
    Out(Addr<OutConnection>),
}

impl Peer {
    pub fn new(port: u32, period: Duration, connect_to: Option<SocketAddr>) -> Self {
        let socket_addr = format!("127.0.0.1:{}", port);
        let socket_addr = SocketAddr::from_str(&socket_addr).unwrap();
        let message = gen_rnd_msg();

        Self {
            socket_addr,
            period,
            message,
            connect_to,
            peers: Default::default(),
            connections: Default::default(),
        }
    }

    fn start_listening(&self, peer_addr: Addr<Peer>) {
        let addr = self.socket_addr.clone();
        spawn(async move {
            let listener = TcpListener::bind(addr).await.unwrap();
            while let Ok((stream, _addr)) = listener.accept().await {
                // replace random client peer port by peer's listening port
                let mut sock_addr = vec![];
                if read_bytes(&stream, &mut sock_addr).await.is_ok() {
                    let addr = read_socket_addr(&stream).await.unwrap();
                    debug!("peer [{addr}] connected");
                    let peer = peer_addr.clone();
                    InConnection::create(|ctx| {
                        let (r, w) = split(stream);
                        // reading from remote peer connection
                        InConnection::add_stream(FramedRead::new(r, InCodec), ctx);
                        // writing to remote peer connection
                        InConnection::new(addr, peer, FramedWrite::new(w, InCodec, ctx))
                    });
                } else {
                    error!["couldn't get remote peer socket address"]
                }
            }
        });
    }
}

impl Actor for Peer {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        // Peer is the first in the network
        if self.connect_to.is_none() {
            info!("Peer started on [{}]. Waiting for incoming connections", self.socket_addr);
            self.start_listening(ctx.address());
            return;
        }
        // Trying to connect initial peer
        self.start_listening(ctx.address());
        let connect_to = self.connect_to.unwrap();
        info!("Peer started on [{}]. Trying to connect [{connect_to}]", self.socket_addr);
        let peer_ctx_address = ctx.address().clone();
        let sock_addr = self.socket_addr;

        ctx.spawn(async move {
            let stream = TcpStream::connect(connect_to).await;
            if let Ok(mut stream) = stream {
                // Tell remote peer our listening address
                let _ = write_socket_addr(&mut stream, sock_addr).await;
                Ok(stream)
            } else {
                Err(io::Error::new(ErrorKind::ConnectionRefused, "couldn't write data to socket"))
            }
        }
            .into_actor(self)
            .map(move |stream, actor, ctx| {
                if stream.is_err() {
                    error!("could not connect to initial peer");
                    ctx.stop();
                } else {
                    let stream = stream.unwrap();
                    let remote_peer_addr = stream.peer_addr().unwrap();
                    let (r, w) = split(stream);
                    let initial_peer = OutConnection::create(|ctx| {
                        OutConnection::add_stream(FramedRead::new(r, OutCodec), ctx);
                        OutConnection::new(
                            actor.socket_addr,
                            remote_peer_addr,
                            peer_ctx_address,
                            FramedWrite::new(w, OutCodec, ctx),
                        )
                    });
                    // try perform handshake with remote peer
                    let _ = initial_peer.try_send(
                        OutMessage::Request(
                            TryHandshake{
                                token: b"secret".to_vec(),
                                sender: actor.socket_addr,
                                receiver: connect_to})
                    );
                }
            })
        );
    }
}

#[derive(Debug, Message)]
#[rtype(result = "()")]
pub struct AddInConnection(pub Addr<InConnection>);

impl Handler<AddInConnection> for Peer {
    type Result = ();

    fn handle(&mut self, msg: AddInConnection, _ctx: &mut Self::Context) -> Self::Result {
        let _ = self.connections.insert(Connection::In(msg.0));
    }
}

#[derive(Debug, Message)]
#[rtype(result = "()")]
pub(crate) struct AddOutConnection(pub Addr<OutConnection>);

impl Handler<AddOutConnection> for Peer {
    type Result = ();

    fn handle(&mut self, msg: AddOutConnection, _ctx: &mut Self::Context) -> Self::Result {
        let _ = self.connections.insert(Connection::Out(msg.0));
    }
}

#[derive(Debug, Message)]
#[rtype(result = "()")]
pub struct AddPeer(pub SocketAddr);

impl Handler<AddPeer> for Peer {
    type Result = ();

    fn handle(&mut self, msg: AddPeer, _ctx: &mut Self::Context) -> Self::Result {
        self.peers.insert(msg.0);
    }
}

#[derive(Debug, Message)]
#[rtype(result = "()")]
pub struct AddPeers(pub HashSet<SocketAddr>);

impl Handler<AddPeers> for Peer {
    type Result = ();

    fn handle(&mut self, msg: AddPeers, ctx: &mut Self::Context) -> Self::Result {
        let mut peers_to_add = msg.0;
        // we don't need to store our own peer address here
        peers_to_add.remove(&self.socket_addr);
        self.peers.extend(peers_to_add);
        let peers_to_connect = self.peers.clone();
        // debug!("peers hash set: {:?}", &self.peers);
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
        let peer_actor = peer_ctx.address();
        // We don't need to add already connected peers
        let peers_to_connect = msg.0.iter()
            .filter_map(|to_connect| {
                return if self.peers.iter().any(|connected| connected.eq(to_connect)) {
                    Some(*to_connect)
                } else { None }
            })
            .collect::<HashSet<SocketAddr>>();

        let peers_to_connect = Arc::new(peers_to_connect);
        let peer_addr = self.socket_addr;

        peer_ctx.spawn(async move {
            // TODO define retry count
            let mut errors = HashSet::new();
            for peer in peers_to_connect.iter() {
                let stream = TcpStream::connect(peer).await;
                if let Ok(mut stream) = stream {
                    let _ = write_socket_addr(&mut stream, peer_addr).await
                        .map_err(|e| {
                            error!["error: {e}"];
                        });
                    OutConnection::create(|ctx| {
                        let (r, w) = split(stream);
                        OutConnection::add_stream(FramedRead::new(r, OutCodec), ctx);
                        OutConnection::new(
                            peer_addr,
                            *peer,
                            peer_actor.clone(),
                            FramedWrite::new(w, OutCodec, ctx)
                        )
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
                if !errors.is_empty() {
                    debug!("retrying to establish connection with peers: {:?} after 3 seconds", errors);
                    ctx.notify_later(ConnectPeers(errors), Duration::from_secs(3));
                }
            }));
    }
}

#[derive(Debug, Message)]
#[rtype(result = "HashSet<SocketAddr>")]
pub(crate) struct GetPeers;

impl Handler<GetPeers> for Peer {
    type Result = MessageResult<GetPeers>;

    fn handle(&mut self, _msg: GetPeers, _ctx: &mut Self::Context) -> Self::Result {
        MessageResult(self.peers.clone())
    }
}

/// send random messages to connected peers
#[derive(Debug, Message)]
#[rtype(result = "()")]
pub struct SendMessages;

impl Handler<SendMessages> for Peer {
    type Result = ();

    fn handle(&mut self, _msg: SendMessages, ctx: &mut Self::Context) -> Self::Result {
        // start sending messages with specified [`period`]
        let msg = self.message.clone();
        ctx.run_interval(self.period, move |actor, _ctx| {
            for conn in actor.connections.iter() {
                match conn {
                    Connection::In(c) => {
                        let _ = c.try_send(InMessage::Request(MessageRequest(msg.clone(), actor.socket_addr)));
                    }
                    Connection::Out(c) => {
                        let _ = c.try_send(OutMessage::Request(MessageRequest(msg.clone(), actor.socket_addr)));
                    }
                }
            }
        });
    }
}

async fn write_socket_addr(socket: &mut TcpStream, addr: SocketAddr) -> Result<(), Error> {
    let byte_buf = serialize_data(&addr)?;
    let data_buf_len = (byte_buf.len() as u16).to_be_bytes();
    write_bytes(socket, &data_buf_len).await?;
    write_bytes(socket, &byte_buf).await?;
    Ok(())
}

async fn read_socket_addr(socket: &TcpStream)  -> Result<SocketAddr, Error> {
    let mut len_buf = [0, 0];
    read_bytes(socket, &mut len_buf).await?;
    let len = u16::from_be_bytes(len_buf);
    let mut data_buf = vec![0; len as _];
    read_bytes(socket, &mut data_buf).await?;
    let sock_addr = deserialize_data::<SocketAddr>(&data_buf)?;
    Ok(sock_addr)
}


pub(crate) async fn read_bytes(s: &TcpStream, buf: &mut [u8]) -> io::Result<()> {
    let mut red = 0;
    while red < buf.len() {
        s.readable().await?;
        match s.try_read(&mut buf[red..]) {
            Ok(0) => break,
            Ok(n) => {
                red += n;
            }
            Err(ref e) if e.kind() == ErrorKind::WouldBlock => { continue; }
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

pub(crate) async fn write_bytes(stream: &TcpStream, buf: &[u8]) -> io::Result<()> {
    let mut written = 0;
    while written < buf.len() {
        stream.writable().await?;
        match stream.try_write(&buf[written..]) {
            Ok(0) => break,
            Ok(n) => {
                written += n;
            }
            Err(ref e) if e.kind() == ErrorKind::WouldBlock => { continue; }
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

fn gen_rnd_msg() -> String {
    use random_word::Lang;
    let msg = random_word::gen(Lang::En);
    String::from(msg)
}