#![feature(async_closure)]

use std::collections::HashSet;
use std::fmt::{Display, Formatter};
use std::io::{Bytes, Error, ErrorKind};
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::io;
use crate::error::{ConnectError, RequestError, RequestResult, SendError};
use crate::network::{Message, read_exact_async, read_msg, request_peers, respond_peers, try_connect, write_msg, write_msg_arc};

const HOST: &str = "127.0.0.1";

/// base struct for networking interaction
#[derive(Debug)]
pub(crate) struct Peer {
    port: u32,
    /// to accept incoming connections
    receiver: Receiver,
    /// to send data to other peers
    sender: Sender,
    /// peer_id / addr
    peers: HashSet<SocketAddr>,
}

impl Peer {

    pub fn new(port: u32) -> Self {
        let (tx, rx) = tokio::sync::mpsc::channel(20);
        let receiver = Receiver::new(tx, port);
        let peers = Default::default();
        let sender = Sender::new(rx);
        Peer {
            port,
            receiver,
            sender,
            peers,
        }
    }
    /// Start listening incoming connections on 'port' with 'period' in secs.
    /// The 'connect_to' arg is None if this peer is first in the network
    ///
    /// # Errors
    /// If unable to start listening on specified `port`
    /// format `address:port`.
    #[allow(unused_doc_comments)]
    pub async fn start(&mut self, period: Duration, connect_to: Option<String>) -> RequestResult<()> {
        let addr = String::from(HOST) + &self.port.to_string();
        println!("My address is : {}", &addr);
        if connect_to.is_some() {
            let peer = SocketAddr::from_str(connect_to.as_ref().unwrap())
                .map_err(|e| RequestError::Send(SendError::Io(Error::from(ErrorKind::InvalidData))))?;
            match try_connect(peer).await {
                Ok(mut stream) => {
                    let peers = request_peers(&mut stream).await?;
                    let connected = Arc::new(Mutex::new(vec![]));
                    for peer in peers.iter() {
                        if let Ok(peer_conn) = try_connect(peer).await {
                            let mut connected = connected.lock().unwrap();
                            connected.push(peer_conn);
                            self.peers.insert(*peer);
                        } else {
                            println!("couldn't connect to peer {peer}")
                        }
                    }
                    {
                        // This scope is needed for dropping MutexGuard<Vec<TcpStream>>
                        let mut connected = connected.lock().unwrap();
                        let output = format!(
                            "Connected to the peers at [{}]",
                            connected.iter()
                                .map(|stream| stream.peer_addr().unwrap().to_string())
                                .fold(String::from("[\"") ,|acc, addr| acc.clone() + ", " + &addr)
                        ) + "\"]";
                        println!("{output}");
                    }
                    /// After connections have been established, start sending messages
                    /// to each peer every [`period`] time interval
                    let mut connected = connected.lock().unwrap();
                    for mut peer_stream in connected.iter() {
                        let peer_stream = Arc::new(Mutex::new(peer_stream));
                        tokio::spawn(async move {
                            loop {
                                let msg = gen_rnd_msg();
                                // TODO here we need reliable error handling
                                if let Err(e) = write_msg_arc(peer_stream, &msg).await {
                                    println!("error writing message: {e}");
                                    break
                                }
                            }
                        });
                    }
                },
                Err(e) => {
                    let peer = connect_to.unwrap();
                    println!("Error while trying to connect to peer: {peer}, error: {e}");
                    return Err(RequestError::Send(SendError::Io(Error::from(ErrorKind::InvalidData))))
                }
            }
        } else {
            // start listening incoming connections
        }
        Ok(())
    }

    /// Send message to peer with specified [`period`] time interval
    async fn send_message_to_peer(&self, period: Duration) -> RequestResult<()> {

    }

    async fn process_incoming_message(&self, socket: &mut TcpStream) -> RequestResult<()> {
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
                Ok(respond_peers(socket, &self.peers).await?)
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
            return Err(Error::new(ErrorKind::Other, "oh no!"))
        } else {
            let msg = gen_rnd_msg();
            for peer in peers.iter() {
                let mut stream = TcpStream::connect(peer).await?;
                write_msg(&mut stream, &msg).await?;
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


// #[derive(Debug)]
// struct Message {
//     text: String,
//     from: SocketAddr
// }
//
// impl Display for Message {
//     fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
//         write!(f, "(Received message '{}' from {})", &self.text, &self.from)
//     }
// }

fn gen_rnd_msg() -> String {
    use random_word::Lang;
    let msg = random_word::gen(Lang::En);
    String::from(msg)
}