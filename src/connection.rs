use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::io;
use std::net::SocketAddr;
use actix::{Actor, ActorContext, ActorFutureExt, Addr, AsyncContext, Context, ContextFutureSpawner, Handler, StreamHandler, WrapFuture};
use actix::io::{FramedWrite, WriteHandler};
use tokio::io::WriteHalf;
use tokio::net::TcpStream;
use tracing::{debug, info};
use tracing::log::error;
use crate::codec::{OutCodec, InCodec};
use crate::message::{InMessage, OutMessage};
use crate::message::Request::{MessageRequest, PeersRequest, TryHandshake};
use crate::message::Response::{AcceptHandshake, PeersResponse};


use crate::peer::{AddPeers, AddOutConnection, GetConnectedPeers, Peer, AddConnectedPeer, SendMessages, AddInConnection};

/// Connection initiated by remote peer
pub struct InConnection {
    /// remote peer [`SocketAddr`].
    /// Actual address is arbitrary address chosen by OS during the TCP connection process.
    /// Substituted by peer listening address for clarification purpose.
    pub remote_peer_addr: SocketAddr,
    /// remote peer [`Actor`] address
    peer_actor: Addr<Peer>,
    /// stream to write messages to remote peer
    write: FramedWrite<InMessage, WriteHalf<TcpStream>, InCodec>,
}

impl InConnection {
    pub fn new(
        peer_addr: SocketAddr,
        peer_actor: Addr<Peer>,
        write: FramedWrite<InMessage, WriteHalf<TcpStream>, InCodec>) -> Self {
        Self {
            remote_peer_addr: peer_addr,
            peer_actor,
            write,
        }
    }
}

impl Actor for InConnection {
    type Context = Context<Self>;

    // Notify peer actor about creation of new connection actor
    fn started(&mut self, ctx: &mut Self::Context) {
        let actor_addr = ctx.address();
        let _ = self.peer_actor.try_send(AddInConnection(actor_addr, self.remote_peer_addr));
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        info!("Connection with [{}] has lost", self.remote_peer_addr)
    }
}

/// Encode messages to send them to incoming network connections
impl Handler<InMessage> for InConnection {
    type Result = ();

    fn handle(&mut self, msg: InMessage, _ctx: &mut Self::Context) -> Self::Result {
        match msg {
            InMessage::Request(req) => {
                match req {
                    MessageRequest(msg, addr) => {
                        info!("sending message [{}] to [{}]", &msg, self.remote_peer_addr);
                        self.write.write(InMessage::Request(MessageRequest(msg, addr)))
                    }
                    PeersRequest => {
                        // send request of all connected peers
                        self.write.write(InMessage::Request(PeersRequest));
                    }
                    TryHandshake {token, sender, receiver} => {
                        // Try to perform handshake
                        self.write.write(InMessage::Request(TryHandshake{ token, sender, receiver}));
                    }
                }
            },
            InMessage::Response(resp) => {
                match resp {
                    PeersResponse(peers) => {
                        self.write.write(InMessage::Response(PeersResponse(peers)))
                    }
                    AcceptHandshake(result) => {
                        self.write.write(InMessage::Response(AcceptHandshake(result)))
                    }
                }
            },
        }
    }
}

/// Decode and process requests from incoming connections
impl StreamHandler<Result<InMessage, io::Error>> for InConnection {
    fn handle(&mut self, item: Result<InMessage, io::Error>, ctx: &mut Self::Context) {
        match item {
            Ok(in_msg) => {
                match in_msg {
                    InMessage::Request(req) => {
                        match req {
                            MessageRequest(msg, addr) => {
                                info!("received message [{msg}] from [{addr}]");
                            }
                            PeersRequest => {
                                self.peer_actor
                                    .send(GetConnectedPeers)
                                    .into_actor(self)
                                    .then(|res, actor, _ctx| {
                                        match res {
                                            Ok(peers) => {
                                                actor.write.write(InMessage::Response(PeersResponse(peers)));
                                            }
                                            _ => debug!("Something is wrong"),
                                        }
                                        actix::fut::ready(())
                                    })
                                    .wait(ctx)
                            }
                            TryHandshake {token, sender, ..} => {
                                //debug!("InConnection received handshake request from [{sender}] to [{receiver}]");
                                let validated = validate_handshake(&token);
                                // add sender to peers list in successful case
                                if validated {
                                    let _ = self.peer_actor.try_send(AddConnectedPeer(sender));
                                } else {
                                    error!{"Handshake failed"}
                                }
                                self.write.write(InMessage::Response(AcceptHandshake(validated)));
                                // start sending messages to connected peer
                                let _ = self.peer_actor.try_send(SendMessages);
                            }
                        }
                    }
                    InMessage::Response(resp) => {
                        debug!("in connection : got response from peer");
                        match resp {
                            PeersResponse(mut peers) => {
                                // debug!("StreamHandler for InConnection PeersResponse {peers:?}");
                                peers.remove(&self.remote_peer_addr);
                                // debug!("&self.remote_peer_addr {}", &self.remote_peer_addr);
                                self.peer_actor.send(AddPeers(peers))
                                    .into_actor(self)
                                    .then(|res, _actor, ctx| {
                                        if res.is_err() {
                                            error!("couldn't add peers: {}", res.err().unwrap());
                                            ctx.stop();
                                        }
                                        actix::fut::ready(())
                                    })
                                    .wait(ctx);
                            }
                            AcceptHandshake(result) => {
                                if !result {
                                    error!("Could not perform handshake!");
                                    ctx.stop();
                                } else {
                                    // Add connected peer to peer's set
                                    let _ = self.peer_actor.try_send(AddConnectedPeer(self.remote_peer_addr));
                                    // request all the other peers
                                    let _ = ctx.address().try_send(InMessage::Request(PeersRequest));
                                }
                            }
                        }
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

/// Connection initiated by current peer
pub struct OutConnection {
    /// Address of this peer.It's needed for being discoverable by other peers.
    peer_addr: SocketAddr,
    /// remote peer [`SocketAddr`]
    remote_peer_addr: SocketAddr,
    /// remote peer [`Actor`] address
    peer_actor: Addr<Peer>,
    /// stream to write messages to remote peer
    write: FramedWrite<OutMessage, WriteHalf<TcpStream>, OutCodec>,
}

impl OutConnection {
    pub fn new(
        peer_addr: SocketAddr,
        remote_peer_addr: SocketAddr,
        peer_actor: Addr<Peer>,
        write: FramedWrite<OutMessage, WriteHalf<TcpStream>, OutCodec>) -> Self {

        Self {
            peer_addr,
            remote_peer_addr,
            peer_actor,
            write,
        }
    }
}

impl Actor for OutConnection {
    type Context = Context<Self>;

    // Notify peer actor about creation of new connection actor
    fn started(&mut self, ctx: &mut Self::Context) {
        let addr = ctx.address();
        let _ = self.peer_actor.try_send(AddOutConnection(addr, self.remote_peer_addr));
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        info!("Connection with [{}] has lost", self.remote_peer_addr)
    }
}

/// Decode and process responses from outgoing connections
impl StreamHandler<Result<OutMessage, io::Error>> for OutConnection {
    fn handle(&mut self, item: Result<OutMessage, io::Error>, ctx: &mut Self::Context) {
        match item {
            Ok(out_msg) => {
                match out_msg {
                    OutMessage::Request(req) => {
                        match req {
                            MessageRequest(msg, sender) => {
                                info!("received message [{msg}] from [{sender}]");
                            }
                            PeersRequest => {
                                self.peer_actor
                                    .send(GetConnectedPeers)
                                    .into_actor(self)
                                    .then(|res, actor, _ctx| {
                                        match res {
                                            Ok(peers) => {
                                                actor.write.write(OutMessage::Response(PeersResponse(peers)));
                                            }
                                            _ => debug!("Something is wrong"),
                                        }
                                        actix::fut::ready(())
                                    })
                                    .wait(ctx)
                            }
                            TryHandshake {token, sender, ..} => {
                                //debug!("OutConnection received handshake request from [{receiver}] to [{sender}]");
                                let validated = validate_handshake(&token);
                                // add sender to peers list in successful case
                                if validated {
                                    let _ = self.peer_actor.try_send(AddConnectedPeer(sender));
                                } else {
                                    error!{"Handshake failed"}
                                }
                                self.write.write(OutMessage::Response(AcceptHandshake(validated)));
                            }
                        }
                    }
                    OutMessage::Response(resp) => {
                        //debug!("out connection : got response from peer");
                        match resp {
                            PeersResponse(mut peers) => {
                                let removed = peers.remove(&self.remote_peer_addr);
                                self.peer_actor.send(AddPeers(peers))
                                    .into_actor(self)
                                    .then(|res, _actor, ctx| {
                                        if res.is_err() {
                                            error!("couldn't add peers: {}", res.err().unwrap());
                                            ctx.stop();
                                        }
                                        actix::fut::ready(())
                                    })
                                    .wait(ctx);
                                // start sending messages with specified period
                                let _ = self.peer_actor.try_send(SendMessages);
                            }
                            AcceptHandshake(result) => {
                                if !result {
                                    error!("Could not perform handshake!");
                                    // TODO check
                                    ctx.stop();
                                } else {
                                    let _ = self.peer_actor.try_send(AddConnectedPeer(self.remote_peer_addr));
                                    // request all the other peers
                                    let _ = ctx.address().try_send(OutMessage::Request(PeersRequest));
                                }
                            }
                        }
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

/// Encode messages for outgoing connections
impl Handler<OutMessage> for OutConnection {
    type Result = ();

    fn handle(&mut self, msg: OutMessage, _ctx: &mut Self::Context) -> Self::Result {
        //debug!("Handler<OutMessage> for OutgoingConnection");
        match msg {
            OutMessage::Request(req) => {
                match req {
                    MessageRequest(msg, addr) => {
                        info!("sending message [{}] to [{}]", &msg, self.remote_peer_addr);
                        self.write.write(OutMessage::Request(MessageRequest(msg, addr)))
                    }
                    PeersRequest => {
                        self.write.write(OutMessage::Request(PeersRequest))
                    }
                    TryHandshake {token, sender, receiver} => {
                        // Try to perform handshake
                        self.write.write(OutMessage::Request(TryHandshake {token, sender, receiver}));
                    }
                }
            },
            OutMessage::Response(resp) => {
                match resp {
                    PeersResponse(peers) => {
                        debug!("OutMessage::Response(resp) PeersResponse(peers) : {peers:?}");
                        self.write.write(OutMessage::Response(PeersResponse(peers)))
                    }
                    AcceptHandshake(result) => {
                        self.write.write(OutMessage::Response(AcceptHandshake(result)))
                    }
                }
            },
        }
    }
}

/// handshake emulation
fn validate_handshake(token: &[u8]) -> bool {
    token == b"secret"
}

impl Hash for InConnection {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.remote_peer_addr.hash(state);
    }
}

impl PartialEq for InConnection {
    fn eq(&self, other: &Self) -> bool {
        self.remote_peer_addr == other.remote_peer_addr
    }
}

impl Eq for InConnection {}

impl Hash for OutConnection {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.remote_peer_addr.hash(state);
    }
}

impl PartialEq for OutConnection {
    fn eq(&self, other: &Self) -> bool {
        self.remote_peer_addr == other.remote_peer_addr
    }
}

impl Eq for OutConnection {}

impl WriteHandler<std::io::Error> for InConnection {}

impl WriteHandler<std::io::Error> for OutConnection {}