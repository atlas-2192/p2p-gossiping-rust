use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt::{Display, Formatter};
use std::io::{Bytes, ErrorKind};
use std::net::SocketAddr;
use std::str::FromStr;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::io;

const HOST: &str = "127.0.0.1";

/// base struct for networking interaction
#[derive(Debug)]
pub(crate) struct Peer {
    /// to accept incoming connections
    listener: TcpListener,
    /// to send data to other peers
    sender: Sender,
    /// peer_id / addr
    peers: HashSet<SocketAddr>,
    // Here we need concurrent collection
    messages: VecDeque<Message>,
}

impl Peer {
    pub fn new() -> Self {

    }
    /// Start listening incoming connections on 'port' with 'period' in secs.
    /// The 'connect_to' arg is None if this peer is first in the network
    ///
    /// # Errors
    /// If unable to start listening on specified `port`
    /// format `address:port`.
    pub async fn start(
        period: Duration,
        port: u32,
        connect_to: Option<SocketAddr>) -> io::Result<()> {

        let addr = SocketAddr::from_str(&(String::from(HOST) + &port.to_string()))
            .expect("Incorrect port number");
        let listener = TcpListener::bind(&addr).await?;

        let peer = Peer {
            listener,
            sender: Sender,
            peers: Default::default(),
            messages: Default::default(),
        };

        println!("My address is : {}", &addr);

        Ok(())
    }
}

/// struct for sending messages to the network
#[derive(Debug)]
struct Sender {
    /// receive incoming message to resend it to the other peers
    rx: tokio::sync::mpsc::Receiver<Message>,

}

// impl Sender {
// 
//     fn new(rx: tokio::sync::mpsc::Receiver<Message>) -> Self {
//         Self { rx }
//     }
//     async fn process(&mut self, peers: HashSet<SocketAddr>) -> Result<(), io::Error> {
//         while let Ok(msg) = self.rx.recv().await {
//             
//         }
//     }
// }

/// struct for receiving messages from other peers
#[derive(Debug)]
struct Receiver {
    ///
    tx: tokio::sync::mpsc::Sender<Message>,
}


#[derive(Debug)]
struct Message {
    text: String,
    from: SocketAddr
}

impl Display for Message {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "(Received message '{}' from {})", &self.text, &self.from)
    }
}

pub(crate) async fn read_exact_async(s: &TcpStream, buf: &mut [u8]) -> io::Result<()> {
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

pub(crate) async fn write_all_async(stream: &TcpStream, buf: &[u8]) -> io::Result<()> {
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