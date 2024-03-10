use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use actix::{
    Actor,
    ActorContext,
    ActorFutureExt,
    ActorTryFutureExt,
    AsyncContext,
    Context,
    fut,
    Handler,
    Message,
    MessageResult,
    Running,
    StreamHandler,
    WrapFuture
};
//use scc::HashSet;
use std::collections::HashSet;
use std::io::{Error, ErrorKind};
use futures_util::TryStreamExt;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tracing::{debug, error, info, warn};
use tracing::field::debug;
use uuid::Uuid;
use crate::{gen_rnd_msg, network};
use crate::network::{read_message, send_message, try_handshake};
use crate::peer::{Connection, Peer, RemotePeer};

/// Newly established connection
#[derive(Debug, Message)]
#[rtype(result = "()")]
pub struct NewConnection(pub(crate) TcpStream);

#[derive(Debug, Message)]
#[rtype(result = "()")]
pub(crate) struct ReadIncomingMessages {
    conn_id: Uuid,
    peer_addr: SocketAddr,
    read: OwnedReadHalf,
}

#[derive(Debug, Message)]
#[rtype(result = "Result<(), std::io::Error>")]
pub(crate) struct WriteOutgoingMessages {}

impl StreamHandler<NewConnection> for Peer {
    fn handle(&mut self, new_conn: NewConnection, ctx: &mut Self::Context) {
        let peer_addr = new_conn.0.peer_addr().unwrap();
        if self.socket_addr == peer_addr {
            debug!("there is no need to add our socket address to remote connections")
        } else if !self.peers_to_connect.insert(peer_addr) {
            warn!("Couldn't add peer to connections as it's been already added")
        } else {
            debug!("new connection is being handled: {:?}", peer_addr);
            let conn = Arc::new(Connection::new(new_conn.0, self.period));
            let mut peer = RemotePeer::Connecting(conn.clone());
            ctx.wait(async move {
                peer = handshake(peer).await;
            }
                .into_actor(self));
            if !self.peers.insert(peer) { debug!("peer is already added") };
        }

        // let (read, write) = new_conn.0.into_split();
        // // start handling incoming connections
        // ctx.notify(ReadIncomingMessages {
        //     conn_id,
        //     peer_addr,
        //     read
        // });
        // // start sending messages
        // ctx.notify(WriteOutgoingMessages {
        //     conn_id,
        //     peer_addr,
        //     write,
        //     period: self.period
        // });
    }
}

async fn handshake(peer: RemotePeer) -> RemotePeer {
    match peer {
        RemotePeer::Connecting(conn) => {
            let handshake = try_handshake(conn.clone()).await;
            if handshake.is_ok() && handshake.unwrap() == true {
                RemotePeer::Connected(conn)
            } else {
                // TODO handle
                RemotePeer::Error("Handshake error".into())
            }
        },
        RemotePeer::Connected(conn) => { RemotePeer::Connected(conn) }
        RemotePeer::Handshake {..} | RemotePeer::Disconnected => { RemotePeer::Error("wrong peer state".into()) }
        _ => {
            error!("wrong");
            RemotePeer::Error("Handshake error".into())
        }
    }
}

async fn request_peers(conn: Arc<Connection>) -> Result<HashSet<SocketAddr>, std::io::Error> {
    network::request_peers(&conn.read, &conn.write).await
}


impl Handler<ReadIncomingMessages> for Peer {
    type Result = ();

    fn handle(&mut self, mut msg: ReadIncomingMessages, ctx: &mut Self::Context) -> Self::Result {
        ctx.spawn(async move {
            loop {
                if let Ok(rnd_msg) = read_message(&mut msg.read).await {
                    info!("received message [{rnd_msg}] from [{}]", msg.peer_addr);
                } else {
                    // TODO handle error
                    break
                }
            }
        }
                      .into_actor(self)
                  //.then(|actor| {})
        );
    }
}

impl Handler<WriteOutgoingMessages> for RemotePeer {
    type Result = Result<(), std::io::Error>;

    fn handle(&mut self, _msg: WriteOutgoingMessages, ctx: &mut Self::Context) -> Self::Result {
        match self {
            RemotePeer::Connecting(_) |
            RemotePeer::Handshake { .. } |
            RemotePeer::Error(_) |
            RemotePeer::Disconnected => Err(Error::new(ErrorKind::Other, "oh no Disconnected!")),
            RemotePeer::Connected(conn) => {
                ctx.spawn(async move {
                    // TODO check 'static
                    let rnd_msg = gen_rnd_msg();
                    loop {
                        tokio::time::sleep(conn.period).await;
                        let rnd_msg = rnd_msg.clone();
                        info!("sending message [{rnd_msg}] to [{}]", conn.peer_addr);
                        if let Err(e) = send_message(&mut conn.write, &rnd_msg).await {
                            // TODO handle errors
                            error!("Couldn't send message: {rnd_msg}, error: {e}");
                            break
                        }
                    }
                }
                              .into_actor(self)
                          //.then(|actor| {})
                );
                Ok(())
            }
        }
    }
}

impl Actor for Peer {
    type Context = Context<Self>;

    #[allow(unused_doc_comments)]
    fn started(&mut self, ctx: &mut Context<Self>) {
        // Start peer with --connect flag: trying establish connection
        if self.connect_to.is_some() {
            let connect_to = self.connect_to.unwrap();
            debug!("Trying to connect to initial peer {connect_to}");
            let period = self.period;
            ctx.wait(async move {
                TcpStream::connect(connect_to).await
            }
                .into_actor(self)
                .map_err(|err, _actor, ctx| {
                    error!("couldn't establish connection with peer: {}", err);
                    // TODO
                    ctx.stop()
                })
                .then(|stream, actor, ctx| {
                    debug!("connection has been established: {:?}", stream);
                    let stream = stream.unwrap();
                    let conn = Arc::new(Connection::new(stream, self.period));
                    // request other peers
                    let mut peers = HashSet::new();
                    ctx.wait(async move {
                        request_peers(conn).await
                    }
                        .into_actor(self)
                        .map_err(|e, _actor, ctx| {
                            error!("could not get peers: {}", e);
                            ctx.stop();
                        })
                        .map(move |res, _actor, _ctx| {
                            peers = res.unwrap();
                        })
                    );
                    actor.peers_to_connect = peers;
                    fut::ready(())
                })
            );
            // start sending random messages to all available peers

            self.peers.iter().for_each(|peer| {
                let sent = peer.start().send(WriteOutgoingMessages {});
                debug!("starting sending messages to [{:?}]", peer);
            });
        } else {
            debug!("Peer is the first peer");
        }
        Self::start_listening(self, ctx);
    }

    fn stopping(&mut self, ctx: &mut Self::Context) -> Running {
        info!("Peer is stopping");
        let state = ctx.state();
        // Here I need to implement reconnect logic
        // if .... Running::Continue
        info!("Peer state: {:?}", state);
        Running::Continue
    }

    fn stopped(&mut self, ctx: &mut Context<Self>) {
        println!("Peer is stopped");
    }
}

impl Actor for RemotePeer {
    type Context = Context<Self>;

    // fn started(&mut self, ctx: &mut Self::Context) {
    //     todo!()
    // }
}

// #[derive(Message)]
// #[rtype(result = "Result<(bool, TcpStream), ()>")]
// /// Handshake process emulation
// pub(crate) struct TryHandshake(TcpStream);
//
// impl Handler<TryHandshake> for Peer {
//     type Result = Result<(bool, TcpStream),()>;
//
//     fn handle(&mut self, msg: TryHandshake, ctx: &mut Self::Context) -> Self::Result {
//         debug!("trying to perform handshake with peer [{}]", msg.0.peer_addr().unwrap());
//         let mut res = false;
//         let stream = msg.0;
//         let arc_stream = Arc::new(stream);
//         ctx.wait(async move {
//                     // TODO Here we need check time of waiting for response
//                     if let Ok(handshake) = try_handshake(arc_stream).await {
//                         res = handshake;
//                     }
//                 }
//                      .into_actor(self));
//
//         Ok((res, stream))
//     }
// }

#[derive(Message, Debug)]
#[rtype(result = "Peers")]
pub(crate) struct PeersRequest;

#[derive(Debug)]
pub(crate) struct Peers(pub HashSet<SocketAddr>);

// impl Handler<PeersRequest> for RemotePeer {
//     type Result = MessageResult<PeersRequest>;
//
//     fn handle(&mut self, _msg: PeersRequest, _ctx: &mut Self::Context) -> Self::Result {
//         debug!("handle PeersRequest...");
//         let mut peers = std::collections::HashSet::new();
//         &self.peers_to_connect.scan(|p| { peers.insert(p.peer_addr); () });
//         MessageResult(Peers(peers))
//     }
// }