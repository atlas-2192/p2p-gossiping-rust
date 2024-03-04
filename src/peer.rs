#![feature(async_closure)]

use std::collections::HashSet;
use std::fmt::{Display, Formatter};
use std::io::{Bytes, Error, ErrorKind};
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::{Arc};
use tokio::sync::Mutex;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::io;
use tracing::{debug, error, info};
use tracing::field::debug;
use crate::error::{ConnectError, RequestError, RequestResult, SendError};
use crate::network::{Message, read_exact_async, read_msg, request_peers, respond_peers, try_connect, write_msg, write_msg_arc};

const HOST: &str = "127.0.0.1";

/// base struct for networking interaction
#[derive(Debug)]
pub(crate) struct Peer {
    state: State,
    port: u32,
    /// TcpListener accepting [`Peer`] connections
    //listener: Option<Arc<Mutex<TcpListener>>>,
    listener: Option<TcpListener>,
    /// to accept incoming connections
    receiver: Receiver,
    /// to send data to other peers
    sender: Sender,
    /// Peers which are required to be connected to this peer
    peers_to_connect: Arc<Mutex<HashSet<SocketAddr>>>,
    /// Peers which are currently connected to this peer
    connected_peers: Vec<TcpStream>,
}

/// States in which peer can be from start up until disconnection
#[derive(Debug, Clone)]
enum State {
    /// Started, trying to establish connection with initial peer
    /// via --connect flag if is_only = true
    Started { is_only: bool, connect_to: Option<String> },
    /// Waiting for incoming connections if the peer is alone
    Waiting,
    /// The peer has established connections with all available peers
    Connected,
    /// The peer stopped and disconnected
    Disconnected,
}

impl Peer {

    pub fn new(port: u32, connect_to: Option<String>) -> Self {
        let (tx, rx) = tokio::sync::mpsc::channel(20);
        let receiver = Receiver::new(tx, port);
        let peers = Default::default();
        let sender = Sender::new(rx);
        let state = match connect_to {
            None => State::Started {is_only: true, connect_to: None},
            Some(value) => State::Started { is_only: false, connect_to: Some(value)},
        };
        Peer {
            state,
            port,
            listener: None,
            receiver,
            sender,
            peers_to_connect: peers,
            connected_peers: Default::default(),
        }
    }


    /// The 'connect_to' arg is None if this peer is first in the network
    ///
    /// # Errors
    /// If unable to start listening on specified port
    /// format `address:port`.
    #[allow(unused_doc_comments)]
    pub async fn run(&mut self, period: Duration) -> Result<(), Box<dyn std::error::Error + Send + Sync>> { //  -> RequestResult<()>
        let addr = String::from(HOST) + &self.port.to_string();
        info!("My address is : {}", &addr);
        loop {
            let state = &self.state.clone();
            match state {
                /// Peer can start as the first peer, or it can try to connect initial peer
                State::Started { is_only, connect_to } => {
                    let started = Self::handle_started(self, *is_only, connect_to, &addr).await;
                    if let Ok(_) = started {
                        continue
                    } else {
                        let err = started.err().unwrap();
                        error!("Error: {}", err);
                        return Err(err)
                    }
                }
                /// Waiting for incoming connections
                State::Waiting => {

                    // match self.listener.as_ref().unwrap().accept().await {
                    //     Ok((socket, addr)) => {
                    //         info!("new client: {:?}", addr);
                    //
                    //         self.state = State::Connected;
                    //         continue;
                    //     },
                    //     Err(e) => info!("couldn't get client: {:?}", e),
                    // }
                }
                State::Connected => {}
                /// Sending messages to other peers that this
                /// peer has been disconnected (to avoid excessive requests)
                State::Disconnected => {
                    info!("Peer {} is terminating", &addr);
                    break
                }
            }
        }

        Ok(())
    }

    async fn start_listen_connections(&self) {
        //-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        //let listener = listener.clone();
        let listener = Arc::new(Mutex::new(&self.listener));
        let peers = self.peers_to_connect.clone();

        loop {
            let peers = peers.clone();
            let listener = listener.lock().await;
            if let Ok((socket, addr)) = listener.as_ref().unwrap().accept().await {
                let socket = Arc::new(Mutex::new(socket));
                tokio::spawn(async move {
                    debug!("process incoming connection : {addr}");
                    let mut socket = socket.lock().await;
                    let processed = Self::process_incoming(peers, &mut socket).await;
                    debug!("process_incoming: {:?}", processed);
                });
            }
        }
    }

    async fn handle_started(&mut self, is_only: bool, connect_to: &Option<String>, addr: &str)
        -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        return if is_only {
            self.state = State::Waiting;
            let listener = TcpListener::bind(addr).await;
            if let Err(ref err) = listener {
                error!("Error: {err}");
                self.state = State::Disconnected;
                // TODO maybe I should return error?
                return Ok(())
            }
            //let listener = listener.unwrap();
            //self.listener = Some(Arc::new(Mutex::new(listener)));
            self.listener = Some(listener.unwrap());
            Self::start_listen_connections(self).await;
            self.state = State::Waiting;
            Ok(())
        } else {
            let listener = TcpListener::bind(addr).await;
            if let Err(ref err) = listener {
                error!("Error: {err}");
                self.state = State::Disconnected;
                return Ok(())
            }
            // let listener = listener.unwrap();
            // self.listener = Some(Arc::new(Mutex::new(listener)));
            self.listener = Some(listener.unwrap());
            Self::start_listen_connections(self).await;
            // connect to initial peer and get all peers
            let mut peer_conn = try_connect(connect_to.as_ref().unwrap()).await?;
            let peers_to_connect = request_peers(&mut peer_conn).await?;
            // connection with initial peer is already established
            self.connected_peers.push(peer_conn);
            // try to establish connection with all known peers
            let mut output = String::from("[\"");
            for p in peers_to_connect.iter() {
                let peer_conn = try_connect(p).await;
                if let Ok(peer_conn) = peer_conn {
                    output = output + &peer_conn.peer_addr().unwrap().to_string() + "\"";
                    self.connected_peers.push(peer_conn);
                } else {
                    error!("Could not connect to peer : {:?}", peer_conn.err().unwrap());
                }
            }
            output = output + "]";
            info!("Connected to the peers at {}", output);
            self.state = State::Connected;

            Ok(())
        }
    }

    /// Send message to peer with specified [`period`] time interval
    // async fn send_message_to_peer(&self, period: Duration) -> RequestResult<()> {
    //
    // }

    async fn process_incoming(
        peers_to_connect: Arc<Mutex<HashSet<SocketAddr>>>,
        socket: &mut TcpStream) -> io::Result<()> {//-> RequestResult<()> {
        let mut cmd_buf = [0u8; 1];
        read_exact_async(socket, &mut cmd_buf).await?;
        let msg = Message::try_from(cmd_buf[0])?;
        match msg {
            Message::Gossiping => {
                let peer = socket.peer_addr().unwrap();
                Ok(read_msg(socket).await
                    .and_then(|msg| {
                        println!("received message [{msg}] from peer [{peer}]");
                        Ok(())
                    })?)
            },
            Message::PeersRequest => {
                let peers_to_connect = peers_to_connect.lock().await;
                Ok(respond_peers(socket, &peers_to_connect).await?)
            }
        }
    }
}

#[derive(Debug)]
struct Connection {
    stream: TcpStream,
}

/// struct for sending messages to the network
#[derive(Debug)]
struct Sender {
    /// receive incoming message to resend it to the other peers
    rx: tokio::sync::mpsc::Receiver<String>,
    //connections: Option<&'c [Connection]>,
    connections: Option<Vec<TcpStream>>,
}

impl Sender {

    fn new(rx: tokio::sync::mpsc::Receiver<String>) -> Self {
        Self { rx, connections: None}
    }

    async fn process(&mut self) {
        loop {
            while let Some(msg) = self.rx.recv().await {
                println!("{msg}");
                for stream in self.connections.as_mut().unwrap().iter_mut() {
                    let _ = write_msg(stream, &msg).await
                        .map(|_| {
                            let peer_addr = stream.peer_addr().expect("TODO");
                            println!("Sending message [{msg}] to ['{peer_addr}']");
                        })
                        .map_err(|e| println!("error while sending : {e}"));
                }
            }
        }
    }

    async fn send_initial_msg(&mut self, period: Duration, peers: &HashSet<SocketAddr>) -> Result<(), Error> {
        if peers.is_empty() {
            return Err(Error::new(ErrorKind::Other, "oh no send_initial_msg!"))
        } else {
            let msg = gen_rnd_msg();
            for peer in peers.iter() {
                let mut stream = TcpStream::connect(peer).await?;
                let res = write_msg(&mut stream, &msg).await;
                if let Err(res) = res {
                    println!("Error write_msg : {res}");
                }
            }
            Ok(())
        }
    }
}

/// struct for receiving messages from other peers
#[derive(Debug)]
struct Receiver {
    /// channel to transmit incoming messages to ['Sender']
    tx: tokio::sync::mpsc::Sender<String>,
    port: u32,
}

impl Receiver {

    fn new(tx: tokio::sync::mpsc::Sender<String>, port: u32) -> Self {
        Self {
            tx, port
        }
    }

    async fn start(&mut self) {
        let addr = SocketAddr::from_str(&(String::from(HOST) + &self.port.to_string()))
            .expect("Incorrect port number");
        loop {
            while let Ok(listener) = TcpListener::bind(&addr).await {
                while let Ok((mut stream, addr)) = listener.accept().await {
                    if let Ok(msg) = read_msg(&mut stream).await {
                        println!("Received message [{msg}] from '{addr}'");
                    } else {
                        println!("error receiving msg from peer : {addr}");
                    }
                }
            }
        }
    }
}

fn gen_rnd_msg() -> String {
    use random_word::Lang;
    let msg = random_word::gen(Lang::En);
    String::from(msg)
}

// /// Start listening incoming connections on 'port' with 'period' in secs.
//     /// The 'connect_to' arg is None if this peer is first in the network
//     ///
//     /// # Errors
//     /// If unable to start listening on specified `port`
//     /// format `address:port`.
//     #[allow(unused_doc_comments)]
//     pub async fn start(&mut self, period: Duration, connect_to: Option<String>) -> RequestResult<()> {
//         let addr = String::from(HOST) + &self.port.to_string();
//         println!("My address is : {}", &addr);
//         if connect_to.is_some() {
//             let peer = SocketAddr::from_str(connect_to.as_ref().unwrap())
//                 .map_err(|e| RequestError::Send(SendError::Io(Error::from(ErrorKind::InvalidData))))?;
//             match try_connect(peer).await {
//                 Ok(mut stream) => {
//                     let peers = request_peers(&mut stream).await?;
//                     let connected = Arc::new(Mutex::new(vec![]));
//                     for peer in peers.iter() {
//                         if let Ok(peer_conn) = try_connect(peer).await {
//                             let mut connected = connected.lock().unwrap();
//                             connected.push(peer_conn);
//                             self.peers.insert(*peer);
//                         } else {
//                             println!("couldn't connect to peer {peer}")
//                         }
//                     }
//                     {
//                         // This scope is needed for dropping MutexGuard<Vec<TcpStream>>
//                         let mut connected = connected.lock().unwrap();
//                         let output = format!(
//                             "Connected to the peers at [{}]",
//                             connected.iter()
//                                 .map(|stream| stream.peer_addr().unwrap().to_string())
//                                 .fold(String::from("[\"") ,|acc, addr| acc.clone() + ", " + &addr)
//                         ) + "\"]";
//                         println!("{output}");
//                     }
//                     /// After connections have been established, start sending messages
//                     /// to each peer every [`period`] time interval
//                     let mut connected = connected.lock().unwrap();
//                     for mut peer_stream in connected.iter() {
//                         let peer_stream = Arc::new(Mutex::new(peer_stream));
//                         tokio::spawn(async move {
//                             loop {
//                                 let msg = gen_rnd_msg();
//                                 // TODO here we need reliable error handling
//                                 if let Err(e) = write_msg_arc(peer_stream, &msg).await {
//                                     println!("error writing message: {e}");
//                                     break
//                                 }
//                             }
//                         });
//                     }
//                 },
//                 Err(e) => {
//                     let peer = connect_to.unwrap();
//                     println!("Error while trying to connect to peer: {peer}, error: {e}");
//                     return Err(RequestError::Send(SendError::Io(Error::from(ErrorKind::InvalidData))))
//                 }
//             }
//         } else {
//             // start listening incoming connections
//         }
//         Ok(())
//     }