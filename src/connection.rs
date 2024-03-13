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
use crate::message::{InMessage, OutMessage, Request, Response};
use crate::message::actor::ActorRequest;
use crate::message::Request::MessageRequest;
use crate::message::Response::{PeersResponse, MessageResponse};

use crate::peer::{AddPeers, GetPeers, Peer};

/// Connection initiated by remote peer
pub struct IncomingConnection {
    /// remote peer [`SocketAddr`]
    peer_addr: SocketAddr,
    /// remote peer [`Actor`] address
    peer_actor: Addr<Peer>,
    /// stream to write messages to remote peer
    write: FramedWrite<InMessage, WriteHalf<TcpStream>, InCodec>,
}

impl IncomingConnection {
    pub fn new(
        peer_addr: SocketAddr,
        peer: Addr<Peer>,
        write: FramedWrite<InMessage, WriteHalf<TcpStream>, InCodec>) -> Self {
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

/// Encode responses to incoming network connections
impl Handler<InMessage> for IncomingConnection {
    type Result = ();

    fn handle(&mut self, msg: InMessage, ctx: &mut Self::Context) -> Self::Result {
        debug!("Handler<Request> for Connection");
        match msg {
            InMessage::Request(req) => {
                match req {
                    Request::MessageRequest(msg, addr) => {
                        info!("sending message [{}] to [{}]", &msg, self.peer_addr);
                        self.write.write(InMessage::Request(MessageRequest(msg, addr)))
                    }
                    Request::PeersRequest => {
                        self.peer_actor.send(GetPeers)
                            .into_actor(self)
                            .then(|res, conn_actor, _ctx| {
                                match res {
                                    Ok(peers) => {
                                        conn_actor.write.write(InMessage::Response(PeersResponse(peers)));
                                    }
                                    _ => debug!("Something is wrong"),
                                }
                                actix::fut::ready(())
                            })
                            .wait(ctx)
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
                }
            },
        }
    }
}

/// Decode and process requests from incoming connections
impl StreamHandler<Result<InMessage, io::Error>> for IncomingConnection {
    fn handle(&mut self, item: Result<InMessage, io::Error>, ctx: &mut Self::Context) {
        match item {
            Ok(in_msg) => {
                match in_msg {
                    InMessage::Request(req) => {
                        match req {
                            Request::MessageRequest(msg, addr) => {
                                info!("Received message [{msg}] from [{addr}]");
                            }
                            Request::PeersRequest => {
                                self.peer_actor
                                    .send(GetPeers)
                                    .into_actor(self)
                                    .then(|res, conn_actor, _ctx| {
                                        match res {
                                            Ok(peers) => {
                                                conn_actor.write.write(InMessage::Response(PeersResponse(peers)));
                                            }
                                            _ => debug!("Something is wrong"),
                                        }
                                        actix::fut::ready(())
                                    })
                                    .wait(ctx)
                            }
                        }
                    }
                    InMessage::Response(resp) => {
                        debug!("got msg from peer");
                        match resp {
                            Response::PeersResponse(peers) => {
                                self.peer_actor.send(AddPeers(peers))
                                    .into_actor(self)
                                    .then(|res, conn_actor, _ctx| {
                                        if res.is_err() {
                                            debug!("mailbox error: {}", res.err().unwrap());
                                        }
                                        actix::fut::ready(())
                                    })
                                    .wait(ctx);
                            }
                            Response::MessageResponse(_, _) => {
                                unreachable!()
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
pub struct OutgoingConnection {
    /// remote peer [`SocketAddr`]
    peer_addr: SocketAddr,
    /// remote peer [`Actor`] address
    peer_actor: Addr<Peer>,
    /// stream to write messages to remote peer
    write: FramedWrite<OutMessage, WriteHalf<TcpStream>, OutCodec>,
}

impl OutgoingConnection {
    pub fn new(
        peer_addr: SocketAddr,
        peer: Addr<Peer>,
        write: FramedWrite<OutMessage, WriteHalf<TcpStream>, OutCodec>) -> Self {
        Self {
            peer_addr,
            peer_actor:
            peer, write
        }
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

/// Decode and process responses from outgoing connections
impl StreamHandler<Result<OutMessage, io::Error>> for OutgoingConnection {
    fn handle(&mut self, item: Result<OutMessage, io::Error>, ctx: &mut Self::Context) {
        match item {
            Ok(in_msg) => {
                match in_msg {
                    OutMessage::Request(req) => {
                        match req {
                            MessageRequest(msg, sender) => {
                                info!("received message [{msg}] from [sender]");
                            }
                            Request::PeersRequest => {
                                self.peer_actor
                                    .send(GetPeers)
                                    .into_actor(self)
                                    .then(|res, conn_actor, _ctx| {
                                        match res {
                                            Ok(peers) => {
                                                conn_actor.write.write(OutMessage::Response(PeersResponse(peers)));
                                            }
                                            _ => debug!("Something is wrong"),
                                        }
                                        actix::fut::ready(())
                                    })
                                    .wait(ctx)
                            }
                        }
                    }
                    OutMessage::Response(resp) => {
                        debug!("got msg from peer");
                        match resp {
                            PeersResponse(peers) => {
                                self.peer_actor.send(AddPeers(peers))
                                    .into_actor(self)
                                    .then(|res, conn_actor, _ctx| {
                                        if res.is_err() {
                                            debug!("mailbox error: {}", res.err().unwrap());
                                        }
                                        actix::fut::ready(())
                                    })
                                    .wait(ctx);
                            }
                            Response::MessageResponse(_, _) => {
                                debug!("unreachable");
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
impl Handler<OutMessage> for OutgoingConnection {
    type Result = ();

    fn handle(&mut self, msg: OutMessage, _ctx: &mut Self::Context) -> Self::Result {
        debug!("Handler<Request> for Connection");
        match msg {
            OutMessage::Request(req) => {
                match req {
                    MessageRequest(msg, addr) => {
                        info!("sending message [{}] to [{}]", &msg, self.peer_addr);
                        self.write.write(OutMessage::Request(MessageRequest(msg, addr)))
                    }
                    Request::PeersRequest => {
                        self.write.write(OutMessage::Request(Request::PeersRequest))
                    }
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
                }
            },
        }
    }
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

impl WriteHandler<std::io::Error> for IncomingConnection {}


impl WriteHandler<std::io::Error> for OutgoingConnection {}

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
