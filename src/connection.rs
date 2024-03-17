use std::collections::HashSet;
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::io;
use std::io::{ErrorKind, Read};
use std::net::SocketAddr;
use std::time::Duration;
use actix::{Actor, ActorContext, ActorFutureExt, ActorStreamExt, ActorTryFutureExt, Addr, AsyncContext, Context, ContextFutureSpawner, Handler, StreamHandler, WrapFuture};
use actix::io::{FramedWrite, WriteHandler};
use byteorder::BigEndian;
use bytes::buf::{Reader, Writer};
use bytes::BytesMut;
use serde_derive::{Deserialize, Serialize};
use tokio::io::WriteHalf;
use tokio::net::TcpStream;
use tracing::{debug, info, warn};
use tracing::log::error;
use crate::codec::{OutCodec, InCodec, serialize_data};
use crate::gen_rnd_msg;
use crate::message::{InMessage, OutMessage};
use crate::message::Request::{MessageRequest, PeersRequest, TryHandshake};
use crate::message::Response::{AcceptHandshake, PeersResponse};


use crate::peer::{AddPeers, AddOutConnection, GetPeers, Peer, AddConnectedPeer, AddPeer, SendMessages};

/// Connection initiated by remote peer
pub struct InConnection {
    /// remote peer [`SocketAddr`]
    pub peer_addr: SocketAddr,
    /// remote peer [`Actor`] address
    peer_actor: Addr<Peer>,
    /// stream to write messages to remote peer
    write: FramedWrite<InMessage, WriteHalf<TcpStream>, InCodec>,
    /// period in which messages are sent
    period: Duration,
}

impl InConnection {
    pub fn new(
        peer_addr: SocketAddr,
        peer_actor: Addr<Peer>,
        write: FramedWrite<InMessage, WriteHalf<TcpStream>, InCodec>,
        period: Duration) -> Self {
        Self {
            peer_addr,
            peer_actor,
            write,
            period
        }
    }
}

impl Display for InConnection {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "(InConnection: {})", self.peer_addr)
    }
}

impl Actor for InConnection {
    type Context = Context<Self>;

    // Notify peer actor about creation of new connection actor
    fn started(&mut self, ctx: &mut Self::Context) {
        let actor_addr = ctx.address();
        let _ = self.peer_actor.try_send(crate::peer::AddInConnection(actor_addr));
    }

    fn stopped(&mut self, ctx: &mut Self::Context) {
        info!("Connection with [{}] has lost", self.peer_addr)
    }
}

/// Encode messages to send them to incoming network connections
impl Handler<InMessage> for InConnection {
    type Result = ();

    fn handle(&mut self, msg: InMessage, _ctx: &mut Self::Context) -> Self::Result {
        debug!("Handler<InMessage> for IncomingConnection");
        match msg {
            InMessage::Request(req) => {
                match req {
                    MessageRequest(msg, addr) => {
                        info!("sending message [{}] to [{}]", &msg, self.peer_addr);
                        self.write.write(InMessage::Request(MessageRequest(msg, addr)))
                    }
                    PeersRequest => {
                        self.write.write(InMessage::Request(PeersRequest));
                    }
                    TryHandshake {token, sender, receiver} => {
                        // Try to perform handshake
                        debug!("InConnection sending handshake request from [{receiver}] to [{sender}]");
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
                                info!("Received message [{msg}] from [{addr}]");
                            }
                            PeersRequest => {
                                self.peer_actor
                                    .send(GetPeers)
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
                            TryHandshake {token, sender, receiver} => {
                                debug!("InConnection received handshake request from [{sender}] to [{receiver}]");
                                let validated = validate_handshake(&token);
                                // add sender to peers list in successful case
                                if validated {
                                    info!("Handshake validated");
                                    let _ = self.peer_actor.try_send(AddPeer(sender));
                                } else {
                                    info!{"Handshake failed"}
                                }
                                self.write.write(InMessage::Response(AcceptHandshake(validated)));
                                // start sending messages to connected peer
                                let _ = self.peer_actor.try_send(SendMessages(gen_rnd_msg()));
                            }
                        }
                    }
                    InMessage::Response(resp) => {
                        debug!("in connection : got response from peer");
                        match resp {
                            PeersResponse(mut peers) => {
                                // Add the rest of peers, except already connected
                                peers.remove(&self.peer_addr);
                                let _ = self.peer_actor.send(AddPeers(peers))
                                    .into_actor(self)
                                    .then(|res, _actor, ctx| {
                                        if res.is_err() {
                                            error!("couldn't add peers: {}", res.err().unwrap());
                                            ctx.stop();
                                        }
                                        actix::fut::ready(())
                                    })
                                    .wait(ctx);
                                // start sending messages with specified [`period`]
                                let msg = gen_rnd_msg();
                                let _ = self.peer_actor.try_send(SendMessages(msg));
                            }
                            AcceptHandshake(result) => {
                                if !result {
                                    error!("Could not perform handshake!");
                                    // TODO check
                                    ctx.stop();
                                } else {
                                    // request all the other peers
                                    debug!("Handshake accepted successfully");
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
    /// period in which messages are sent
    period: Duration,
}

impl OutConnection {
    pub fn new(
        peer_addr: SocketAddr,
        remote_peer_addr: SocketAddr,
        peer_actor: Addr<Peer>,
        write: FramedWrite<OutMessage, WriteHalf<TcpStream>, OutCodec>,
        period: Duration) -> Self {
        Self {
            peer_addr,
            remote_peer_addr,
            peer_actor,
            write,
            period
        }
    }
}

impl Display for OutConnection {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "(OutConnection: peer_addr: {}, remote_peer_addr: {})", self.peer_addr, self.remote_peer_addr)
    }
}

impl Actor for OutConnection {
    type Context = Context<Self>;

    // Notify peer actor about creation of new connection actor
    fn started(&mut self, ctx: &mut Self::Context) {
        let addr = ctx.address();
        let _ = self.peer_actor.try_send(AddOutConnection(addr));
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
                                    .send(GetPeers)
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
                            TryHandshake {token, sender, receiver} => {
                                debug!("OutConnection received handshake request from [{receiver}] to [{sender}]");
                                let validated = validate_handshake(&token);
                                // add sender to peers list in successful case
                                if validated {
                                    info!("Handshake validated");
                                    self.peer_actor.try_send(AddPeer(sender));
                                } else {
                                    info!{"Handshake failed"}
                                }
                                self.write.write(OutMessage::Response(AcceptHandshake(validated)));
                            }
                        }
                    }
                    OutMessage::Response(resp) => {
                        debug!("out connection : got response from peer");
                        match resp {
                            PeersResponse(mut peers) => {
                                // Add the rest of peers, except already connected
                                peers.remove(&self.peer_addr);
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
                                // start sending messages with specified [`period`]
                                let msg = gen_rnd_msg();
                                let _ = self.peer_actor.try_send(SendMessages(msg));
                            }
                            AcceptHandshake(result) => {
                                if !result {
                                    error!("Could not perform handshake!");
                                    // TODO check
                                    ctx.stop();
                                } else {
                                    debug!("OutConnection handshake accepted successfully");
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
        debug!("Handler<OutMessage> for OutgoingConnection");
        match msg {
            OutMessage::Request(req) => {
                match req {
                    MessageRequest(msg, addr) => {
                        info!("sending message [{}] to [{}]", &msg, self.remote_peer_addr);
                        self.write.write(OutMessage::Request(MessageRequest(msg, addr)))
                    }
                    PeersRequest => {
                        debug!("sending peer request");
                        self.write.write(OutMessage::Request(PeersRequest))
                    }
                    TryHandshake {token, sender, receiver} => {
                        // Try to perform handshake
                        debug!("OutConnection sending handshake request from [{sender}] to [{receiver}]");
                        self.write.write(OutMessage::Request(TryHandshake {token, sender, receiver}));
                    }
                }
            },
            OutMessage::Response(resp) => {
                match resp {
                    PeersResponse(peers) => {
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

/// decrypting emulation
fn validate_handshake(token: &[u8]) -> bool {
    token == b"secret"
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


// /*
//     Protocol commands
//  */
// const REQ_HANDSHAKE: u8 = 1;
//
// const RESP_HANDSHAKE: u8 = 2;
//
// const ACCEPT_HANDSHAKE: u8 = 3;
//
// const PEERS: u8 = 4;
//
// const MESSAGE: u8 = 5;
//
// #[derive(Serialize, Deserialize)]
// struct MessageWithSender {
//     msg: String,
//     sender: SocketAddr,
// }
//
// #[derive(Serialize, Deserialize)]
// struct HandshakeReq {
//     sender: SocketAddr,
//     receiver: SocketAddr,
// }
//
// fn encode_peers_req(mut writer: Writer<&mut BytesMut>) -> io::Result<()> {
//     let cmd_buf = [crate::codec::PEERS; 1];
//     writer.write_all(&cmd_buf)?;
//     Ok(())
//     //Ok(writer.flush()?)
// }
//
// fn encode_msg(mut writer: Writer<&mut BytesMut>, msg: &MessageWithSender) -> io::Result<()> {
//     debug!("codec encode random message");
//     let command = [crate::codec::MESSAGE; 1];
//     writer.write_all(&command)?;
//     warn!("COMMAND [{command:?}] HAS BEEN WRITTEN TO WRITER");
//     let byte_buf = serialize_data(&msg)?;
//     debug!("byte_buf : {byte_buf:?}");
//     let len = byte_buf.len() as u32;
//     writer.write_u32::<BigEndian>(len)?;
//     writer.write_all(&byte_buf)?;
//     debug!("byte_buf: {byte_buf:?}");
//     Ok(())
//     //Ok(writer.flush()?)
// }
//
// fn decode_msg(mut reader: Reader<&mut BytesMut>) -> io::Result<MessageWithSender> {
//     debug!("codec decode random message");
//     let len = reader.read_u32::<BigEndian>()?;
//     let mut bytes_buf = vec![0_u8; len as usize];
//     reader.read(&mut bytes_buf)?;
//     let payload = &bytes_buf[1..bytes_buf.len()];
//     debug!("byte_buf: {bytes_buf:?}");
//     let msg = crate::codec::deserialize_data(&payload)?;
//     Ok(msg)
// }
//
// fn encode_peers(mut writer: Writer<&mut BytesMut>, peers: &HashSet<SocketAddr>) -> io::Result<()> {
//     let command = [crate::codec::PEERS; 1];
//     writer.write_all(&command)?;
//     let byte_buf = serialize_data(peers)?;
//     let len = byte_buf.len() as u32;
//     writer.write_u32::<BigEndian>(len)?;
//     writer.write_all(&byte_buf)?;
//     //writer.flush()?;
//     Ok(())
// }
//
// fn decode_peers(mut reader: Reader<&mut BytesMut>) -> io::Result<HashSet<SocketAddr>> {
//     let len = reader.read_u32::<BigEndian>()?;
//     let mut bytes_buf = vec![0_u8; len as usize];
//     reader.read(&mut bytes_buf)?;
//     let peers = crate::codec::deserialize_data(&bytes_buf)?;
//     Ok(peers)
// }
//
// fn process_req_handshake(mut writer: Writer<&mut BytesMut>, req: &HandshakeReq)
//                          -> io::Result<()> {
//     let command = [crate::codec::REQ_HANDSHAKE; 1];
//     writer.write_all(&command)?;
//     writer.write_all(b"secret")?;
//     let byte_buf = serialize_data(req)?;
//     let len = byte_buf.len() as u32;
//     writer.write_u32::<BigEndian>(len)?;
//     writer.write_all(&byte_buf)?;
//     //writer.flush()?;
//     Ok(())
// }
//
// /// Validate request and return sender socket address
// fn validate_handshake_req(mut reader: Reader<&mut BytesMut>) -> io::Result<HandshakeReq> {
//     let mut buf = [0; 6];
//     reader.read(&mut buf)?;
//     if buf.ne(b"secret") {
//         return Err(io::Error::new(ErrorKind::PermissionDenied, "Handshake error"))
//     }
//     let len = reader.read_u32::<BigEndian>()?;
//     let mut bytes_buf = vec![0_u8; len as usize];
//     reader.read(&mut bytes_buf)?;
//     let req = crate::codec::deserialize_data(&bytes_buf)?;
//     Ok(req)
// }
//
// /// Answer to [`process_req_handshake`]. Returns peer's address answering to handshake request
// fn response_handshake(mut writer: Writer<&mut BytesMut>, success: bool) -> io::Result<()> {
//     debug!("response_handshake() res : {success}");
//     let command = [crate::codec::ACCEPT_HANDSHAKE; 1];
//     writer.write_all(&command)?;
//     match success {
//         true => {
//             writer.write_u8(1)?;
//             //writer.flush()?;
//         },
//         false => {
//             writer.write_u8(0)?;
//             //writer.flush()?;
//         },
//     };
//     Ok(())
// }
//
// /// Validate handshake response
// fn validate_handshake_resp(mut reader: Reader<&mut BytesMut>) -> io::Result<bool> {
//     let mut result_buf = [0; 1];
//     reader.read(&mut result_buf)?;
//     match result_buf[0] {
//         0 => {
//             Ok(false)
//         }
//         1 => {
//             Ok(true)
//         }
//         _ => Err(io::Error::new(ErrorKind::InvalidInput, "Wrong command"))
//     }
// }
