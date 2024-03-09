use std::error::Error;
use scc::HashSet;
use std::fmt::{Display, Formatter};
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io::{BufReader, ErrorKind, Read};
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::{Arc};
use std::time::Duration; // , Mutex

use actix::prelude::*;
use async_stream::stream;
use futures::pin_mut;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tracing::{debug, error, info, warn};
use uuid::Uuid;
use futures::stream::{self, StreamExt};
use futures_util::task::SpawnExt;
use crate::{gen_rnd_msg};
use crate::network::{read_message, send_message};
use crate::actor::NewConnection;


#[derive(Debug)]
pub(crate) struct Peer {
    /// peer's socket address
    socket_addr: SocketAddr,
    /// peer to connect first
    pub(crate) connect_to: Option<SocketAddr>,
    /// to listen incoming connections
    listener: Arc<Mutex<Option<TcpListener>>>,
    /// peers haven't been connected yet
    pub(crate) peers_to_connect: scc::HashSet<Connection>,
    /// active peer's connections
    pub(crate) peer_conns: scc::HashSet<(Connection, PeerState)>,
    /// time interval between sending messages
    pub(crate) period: Duration,
}

/// Remote peer's possible states
#[derive(Hash, Eq, PartialEq, Debug)]
pub(crate) enum PeerState {
    /// Establishing connection with remote peer
    Connecting,
    /// Performing handshake
    Handshake{result: bool},
    /// Receiving incoming messages and sending the own ones
    Connected,
    /// Error occurred while interaction
    Error(String),
    /// Stopping receiving and sending messages and notifying other peers
    Disconnected,
}

/// Connection between this and remote peer
#[derive(Debug)]
pub(crate) struct Connection {
    /// Connection id
    pub(crate) id: Uuid,
    /// remote peer address
    pub(crate) peer_addr: SocketAddr,
}

impl Connection {
    pub(crate) fn new(peer_addr: SocketAddr) -> Self {
        Self {
            id: Uuid::new_v4(),
            peer_addr,
        }
    }
}

impl Hash for Connection {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl PartialEq for Connection {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for Connection {}

impl From<Connection> for SocketAddr {
    fn from(value: Connection) -> Self {
        value.peer_addr
    }
}


impl Peer {
    pub async fn new(period: Duration, port: u32, connect_to: Option<u32>) -> Result<Self, Box<dyn Error>> {
        // TODO for linux 0.0.0.0 - create env
        let socket_addr = format!("127.0.0.1:{}", port);
        let socket_addr = SocketAddr::from_str(&socket_addr).unwrap();
        let listener = Arc::new(Mutex::new(None));

        let arc_listener = listener.clone();
        tokio::spawn(async move {
            let listener2 = arc_listener.clone();
            let mut listener2 = listener2.lock().await;
            // start listening incoming connections
            let _ = listener2.insert(
                TcpListener::bind(socket_addr).await
                    .expect("Couldn't bind TCP listener"));
            debug!("Tcp listener has been bound to `{}`", socket_addr);
        });

        match connect_to {
            Some(connect_to) => {
                // sending connection request to initial peer
                // when flag --connect is applied
                let connect_to_addr = format!("127.0.0.1:{}", connect_to);
                let connect_to_addr = SocketAddr::from_str(&connect_to_addr)?;
                let peers_to_connect = HashSet::default();
                peers_to_connect.insert(Connection::new(connect_to_addr)).expect("Unreachable");
                Ok(Self {
                    socket_addr,
                    connect_to: Some(connect_to_addr),
                    listener,
                    peers_to_connect,
                    peer_conns: Default::default(),
                    period
                })
            },
            None => {
                let peers_to_connect = Default::default();
                Ok(Self {
                    socket_addr,
                    connect_to: None,
                    listener,
                    peers_to_connect,
                    peer_conns: Default::default(),
                    period
                })
            }
        }
    }

    pub(crate) fn start_listening(&mut self, ctx: &mut Context<Self>) {
        debug!("Start listening incoming connections on {}", self.socket_addr);
        let listener = self.listener.clone();
        let listen_addr = self.socket_addr;
        // let (_, mut finish): (oneshot::Sender<NewConnection>, oneshot::Receiver<NewConnection>) = oneshot::channel();
        let conn_stream = stream! {
            tokio::time::sleep(Duration::from_secs(1)).await;
            let mut listener = listener.lock().await;
            let listener = listener.as_mut().unwrap();
            loop {
                // tokio::select! {
                //     accept = listener.accept() => {
                        match listener.accept().await {
                            Ok((stream, addr)) => {
                                debug!(%listen_addr, from_addr = %addr, "Accepted connection");
                                let new_conn = NewConnection(stream);
                                yield new_conn;
                            },
                            Err(error) => {
                                warn!(%error, "Error accepting connection");
                                //yield ConnectAddr(SocketAddr::from_str("").unwrap());
                            }
                        }
                    // }
                    // _ = (&mut finish) => {
                    //     info!("Listening stream finished");
                    //     break;
                    // }
                    // else => break,
                //}
            }
        };
        ctx.add_stream(conn_stream);
    }
}

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
