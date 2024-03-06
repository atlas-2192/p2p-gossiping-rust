use std::collections::{HashSet, VecDeque};
use std::fmt::{Display, Formatter};
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use actix::dev::{MessageResponse, OneshotSender};
use actix::prelude::*;
use tokio::io::WriteHalf;
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, error, info};
use crate::codec::P2PCodec;
use crate::error::ResolverError;

#[derive(Eq, PartialEq, Debug)]
pub struct ConnectAddr(pub SocketAddr);

impl Message for ConnectAddr {
    type Result = Result<TcpStream, ResolverError>;
}

pub struct Peer {
    socket_addr: SocketAddr,
    connect_to: Option<SocketAddr>,
    listener: Option<TcpListener>,
    peers: HashSet<SocketAddr>,
    peer_conns: Vec<Connection>,
}

struct Connection {
    stream: TcpStream,
    addr: Addr<Peer>,
}

pub struct Session {
    /// Unique session id
    id: usize,

    // _framed: actix::io::FramedWrite<WriteHalf<TcpStream>, P2PCodec>,
}
impl Peer {
    pub async fn new(port: u32, connect_to: Option<u32>) -> Self {
        // TODO for linux 0.0.0.0 - create env
        let socket_addr = format!("127.0.0.1:{}", port);
        let socket_addr = SocketAddr::from_str(&socket_addr).unwrap();
        //let listener = TcpListener::bind(socket_addr).await.expect("Couldn't bind TCP listener");

        match connect_to {
            Some(connect_to) => {
                let connect_to_addr = format!("127.0.0.1:{}", connect_to);
                let connect_to_addr = SocketAddr::from_str(&connect_to_addr).unwrap();
                let peers_to_connect = HashSet::from([connect_to_addr]);
                Self {
                    socket_addr,
                    connect_to: Some(connect_to_addr),
                    listener:None,
                    peers: peers_to_connect,
                    peer_conns: Default::default(),
                }
            },
            None => {
                let peers_to_connect = Default::default();
                Self {
                    socket_addr,
                    connect_to: None,
                    listener: None,
                    peers: peers_to_connect,
                    peer_conns: Default::default(),
                }
            }
        }
    }


}

impl Actor for Peer {
    type Context = Context<Self>;

    fn create<F>(f: F) -> Addr<Self>
        where
            Self: Actor<Context=Context<Self>>,
            F: FnOnce(&mut Context<Self>) -> Self
    {
        todo!()
    }

    fn started(&mut self, ctx: &mut Context<Self>) {
        // Start peer with --connect flag: trying establish connection
        if self.connect_to.is_some() {
            let connect_to = self.connect_to.unwrap();
            info!("Trying to connect to initial peer {connect_to}");
            ctx.spawn(async move {
                TcpStream::connect(connect_to).await
            }
                .into_actor(self)
                .map_err(move |err, _actor, ctx| {
                    error!("couldn't establish connection with peer: {}", err);
                    ctx.stop()
                })
                .then(move |stream, actor, ctx| {
                    let stream = stream.unwrap();
                    actor.peer_conns.push(stream);
                    // Getting other peers addr
                    fut::ready(())
                })
            );
        } else {
            info!("Peer is the first peer");
            let socket_addr = self.socket_addr.clone();
            ctx.spawn(async move {
                TcpListener::bind(socket_addr).await
            }
                .into_actor(self)
                .map_err(|err, _actor, ctx| {
                    error!("Cannot bind Tcp listener : {}", err);
                    ctx.stop();
                })
                .map(|listener, actor, _ctx| {
                    debug!("Tcp listener has been bound to `{}`", actor.socket_addr);
                    actor.listener = Some(listener.unwrap());
                })
            );
        }
    }

    fn stopping(&mut self, ctx: &mut Self::Context) -> Running {
        info!("Peer is stopping");
        let state = ctx.state();
        // Here I need to implement reconnect logic
        // if .... Running::Continue
        info!("Peer state: {:?}", state);
        Running::Stop
    }

    fn stopped(&mut self, ctx: &mut Context<Self>) {
        println!("Peer is stopped");
    }
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct RandomMessage {
    pub from: Addr<Peer>,
    pub message: String
}

#[derive(Message, Debug)]
#[rtype(result = "Peers")]
pub struct PeersRequest;

pub struct Peers(pub HashSet<SocketAddr>);

impl Handler<PeersRequest> for Peer {
    type Result = MessageResult<PeersRequest>;

    fn handle(&mut self, _msg: PeersRequest, _ctx: &mut Self::Context) -> Self::Result {
        let peers = self.peers.to_owned();
        MessageResult(Peers(peers))
    }
}

// Spawns a job to execute the given closure periodically, at a specified fixed interval.
// Here I need to spawn job of sending messages
// ctx.run_interval()

impl Handler<RandomMessage> for Peer {
    type Result = ();

    fn handle(&mut self, msg: RandomMessage, _ctx: &mut Context<Self>) -> Self::Result {
        info!("Received message [{}] from {:?}", msg.message, msg.from);
        ()
    }
}

// fn started(&mut self, ctx: &mut Context<Self>) {
//         // Start peer with --connect flag: trying establish connection
//         if self.connect_to.is_some() {
//             let connect_to = self.connect_to.unwrap();
//             info!("Trying to connect to peer {connect_to}");
//             ctx.spawn(async move {
//                 TcpStream::connect(connect_to).await
//             }
//                 .into_actor(self)
//                 .then(|stream, actor, ctx| {
//                     if stream.is_err() {
//                         error!("couldn't establish connection with peer: {connect_to}");
//                         ctx.stop()
//                     } else {
//                         let stream = stream.unwrap();
//                         let conn = Connection {};
//                         actor.peer_conns.push(stream);
//                         // Getting other peers addr
//                     }
//                     fut::ready(())
//                 })
//             );
//         } else {
//             info!("Peer is the first peer");
//         }
//     }

// #[derive(Message)]
// #[rtype(result = "Responses")]
// pub enum Messages {
//     RandomMessage,
//     PeersRequest,
// }
//
// pub enum Responses {
//     // For now keep it here
//     GotRandomMessage,
//     PeersResponse(HashSet<SocketAddr>),
// }
//
// impl Handler<Messages> for Peer {
//     type Result = Responses;
//
//     fn handle(&mut self, msg: Messages, _ctx: &mut Context<Self>) -> Self::Result {
//         let peers = Default::default();
//         match msg {
//             Messages::RandomMessage => Responses::GotRandomMessage,
//             Messages::PeersRequest => Responses::PeersResponse(peers),
//         }
//     }
// }
//
// impl<A, M> MessageResponse<A, M> for Responses
//     where
//         A: Actor,
//         M: Message<Result = Responses>,
// {
//     fn handle(self, ctx: &mut A::Context, tx: Option<OneshotSender<M::Result>>) {
//         if let Some(tx) = tx {
//             let sent = tx.send(self);
//         }
//     }
// }
