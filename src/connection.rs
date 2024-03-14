use std::hash::{Hash, Hasher};
use std::io;
use std::net::SocketAddr;
use actix::{Actor, ActorContext, ActorFutureExt, ActorTryFutureExt, Addr, AsyncContext, Context, ContextFutureSpawner, Handler, StreamHandler, WrapFuture};
use actix::io::{FramedWrite, WriteHandler};
use tokio::io::WriteHalf;
use tokio::net::TcpStream;
use tracing::{debug, info};
use tracing::log::error;
use crate::codec::{OutCodec, InCodec};
use crate::message::{InMessage, OutMessage, Request, Response};
use crate::message::actor::ActorRequest;
use crate::message::Request::MessageRequest;
use crate::message::Response::{PeersResponse, MessageResponse};

use crate::peer::{AddPeers, AddOutConnection, GetPeers, Peer, AddConnectedPeer};

/// Connection initiated by remote peer
pub struct InConnection {
    /// remote peer [`SocketAddr`]
    pub peer_addr: SocketAddr,
    /// remote peer [`Actor`] address
    peer_actor: Addr<Peer>,
    /// stream to write messages to remote peer
    write: FramedWrite<InMessage, WriteHalf<TcpStream>, InCodec>,
}

impl InConnection {
    pub fn new(
        peer_addr: SocketAddr,
        peer: Addr<Peer>,
        write: FramedWrite<InMessage, WriteHalf<TcpStream>, InCodec>) -> Self {
        Self { peer_addr, peer_actor: peer, write }
    }
}

impl Actor for InConnection {
    type Context = Context<Self>;

    // Notify peer actor about creation of new connection actor
    fn started(&mut self, ctx: &mut Self::Context) {
        let actor_addr = ctx.address();
        let socket_addr = self.peer_addr;
        let _ = self.peer_actor.try_send(crate::peer::AddInConnection(actor_addr, socket_addr));
    }
}

/// Encode responses to incoming network connections
impl Handler<InMessage> for InConnection {
    type Result = ();

    fn handle(&mut self, msg: InMessage, ctx: &mut Self::Context) -> Self::Result {
        debug!("Handler<Request> for IncomingConnection");
        match msg {
            InMessage::Request(req) => {
                match req {
                    MessageRequest(msg, addr) => {
                        info!("sending message [{}] to [{}]", &msg, self.peer_addr);
                        self.write.write(InMessage::Request(MessageRequest(msg, addr)))
                    }
                    Request::PeersRequest => {
                        self.peer_actor.send(GetPeers)
                            .into_actor(self)
                            .then(|res, actor, ctx| {
                                match res {
                                    Ok(peers) => {
                                        // Add just connected peer to connections
                                        actor.peer_actor.try_send(AddConnectedPeer(peer_addr));
                                        actor.write.write(InMessage::Response(PeersResponse(peers)));
                                    }
                                    _ => debug!("Something is wrong"),
                                }
                                actix::fut::ready(())
                            })
                            .wait(ctx)
                    }
                    Request::Handshake(addr) => {

                    }
                }
            },
            InMessage::Response(resp) => {
                match resp {
                    PeersResponse(peers) => {
                        self.write.write(InMessage::Response(PeersResponse(peers)))
                    }
                    MessageResponse(_, _) => {
                        unreachable!()
                    }
                    Response::Handshake(result) => {

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
                                info!("Received message [{msg}] from [{addr}]");
                            }
                            Request::PeersRequest => {
                                self.peer_actor
                                    .send(GetPeers)
                                    .into_actor(self)
                                    .then(|res, actor, _ctx| {
                                        match res {
                                            Ok(peers) => {
                                                actor.peer_actor.try_send(AddConnectedPeer(peer_addr));
                                                actor.write.write(InMessage::Response(PeersResponse(peers)));
                                            }
                                            _ => debug!("Something is wrong"),
                                        }
                                        actix::fut::ready(())
                                    })
                                    .wait(ctx)
                            }
                            Request::Handshake(result) => {}
                        }
                    }
                    InMessage::Response(resp) => {
                        debug!("got msg from peer");
                        match resp {
                            PeersResponse(peers) => {
                                // Add the rest of peers, except already connected
                                // peers.remove(&self.peer_addr);
                                self.peer_actor.send(AddPeers(peers))
                                    .into_actor(self).then(|res, conn_actor, _ctx| {
                                    if res.is_err() {
                                        debug!("mailbox error: {}", res.err().unwrap());
                                    }
                                    actix::fut::ready(())
                                }).wait(ctx);
                            }
                            MessageResponse(_, _) => {
                                unreachable!()
                            }
                            Response::Handshake(result) => {

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
        peer: Addr<Peer>,
        write: FramedWrite<OutMessage, WriteHalf<TcpStream>, OutCodec>) -> Self {
        Self {
            peer_addr,
            remote_peer_addr,
            peer_actor:
            peer, write
        }
    }
}

impl Actor for OutConnection {
    type Context = Context<Self>;

    // Notify peer actor about creation of new connection actor
    fn started(&mut self, ctx: &mut Self::Context) {
        let addr = ctx.address();
        let socket_addr = self.remote_peer_addr;
        let _ = self.peer_actor.send(AddOutConnection(addr, socket_addr));
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
                            Request::PeersRequest => {
                                self.peer_actor
                                    .send(GetPeers)
                                    .into_actor(self)
                                    .then(|res, actor, _ctx| {
                                        match res {
                                            Ok(peers) => {
                                                actor.peer_actor.try_send(AddConnectedPeer(peer_addr));
                                                actor.write.write(OutMessage::Response(PeersResponse(peers)));
                                            }
                                            _ => debug!("Something is wrong"),
                                        }
                                        actix::fut::ready(())
                                    })
                                    .wait(ctx)
                            }
                            Request::Handshake() => {}
                        }
                    }
                    OutMessage::Response(resp) => {
                        debug!("got response from peer");
                        match resp {
                            PeersResponse(mut peers) => {
                                // Add the rest of peers, except already connected
                                // peers.remove(&self.peer_addr);
                                self.peer_actor.send(AddPeers(peers))
                                    .into_actor(self)
                                    .then(|res, _actor, _ctx| {
                                        if res.is_err() {
                                            debug!("mailbox error: {}", res.err().unwrap());
                                        }
                                        actix::fut::ready(())
                                    })
                                    .wait(ctx);
                            }
                            MessageResponse(_, _) => {
                                debug!("unreachable");
                            }
                            Response::Handshake(result) => {

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

/// Encode requests for outgoing connections
impl Handler<OutMessage> for OutConnection {
    type Result = ();

    fn handle(&mut self, msg: OutMessage, _ctx: &mut Self::Context) -> Self::Result {
        debug!("Handler<OutMessage> for OutgoingConnection");
        match msg {
            OutMessage::Request(req) => {
                match req {
                    MessageRequest(msg, addr) => {
                        info!("sending message [{}] to [{}]", &msg, self.remote_peer_addr);
                        self.write.write(OutMessage::Request(MessageRequest(msg, addr)))
                    }
                    Request::PeersRequest => {
                        debug!("sending peer request");
                        self.write.write(OutMessage::Request(Request::PeersRequest))
                    }
                    Request::Handshake(result) => {}
                }
            },
            OutMessage::Response(resp) => {
                match resp {
                    PeersResponse(peers) => {
                        self.write.write(OutMessage::Response(PeersResponse(peers)))
                    }
                    MessageResponse(_, _) => {
                        unreachable!()
                    }
                    Response::Handshake(result) => {

                    }
                }
            },
        }
    }
}

impl Hash for InConnection {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.peer_addr.hash(state);
    }
}

impl PartialEq for InConnection {
    fn eq(&self, other: &Self) -> bool {
        self.peer_addr == other.peer_addr
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

// // impl Handler<ActorRequest> for OutgoingConnection {
// //     type Result = ();
// //
// //     fn handle(&mut self, msg: ActorRequest, ctx: &mut Self::Context) -> Self::Result {
// //         debug!("Handler<ActorRequest> for OutgoingConnection");
// //         match msg {
// //             ActorRequest::Message(msg, sock_addr) => {
// //                 self.write.write(OutMessage::Request(RandomMessagePayload(msg, sock_addr)))
// //             }
// //             ActorRequest::PeersRequest => {
// //                 self.write.write(OutMessage::Request(PeersRequest))
// //             }
// //         }
// //     }
// // }
// //
// // impl Handler<ActorResponse> for OutgoingConnection {
// //     type Result = ();
// //
// //     fn handle(&mut self, msg: ActorResponse, ctx: &mut Self::Context) -> Self::Result {
// //         todo!()
// //     }
// // }
//
