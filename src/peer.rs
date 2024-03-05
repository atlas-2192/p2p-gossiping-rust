use std::collections::{HashSet, VecDeque};
use std::fmt::{Display, Formatter};
use std::net::SocketAddr;
use std::str::FromStr;
use actix::dev::{MessageResponse, OneshotSender};
use actix::prelude::*;
use tokio::net::{TcpListener, TcpStream};
use tracing::info;
use crate::error::ResolverError;

#[derive(Eq, PartialEq, Debug)]
pub struct ConnectAddr(pub SocketAddr);

impl Message for ConnectAddr {
    type Result = Result<TcpStream, ResolverError>;
}

pub struct Peer {
    socket_addr: SocketAddr,
    connect_to: Option<SocketAddr>,
    listener: TcpListener,
    peers_to_connect: HashSet<SocketAddr>,
    connections: HashSet<TcpStream>,
}
impl Peer {
    pub async fn new(port: u32, connect_to: Option<u32>) -> Self {
        // TODO for linux 0.0.0.0 - create env
        let socket_addr = format!("127.0.0.1:{}", port);
        let socket_addr = SocketAddr::from_str(&socket_addr).unwrap();
        let listener = TcpListener::bind(socket_addr).await.expect("Couldn't bind TCP listener");

        match connect_to {
            Some(connect_to) => {
                let connect_to_addr = format!("127.0.0.1:{}", connect_to);
                let connect_to_addr = SocketAddr::from_str(&connect_to_addr).unwrap();
                let peers_to_connect = HashSet::from([connect_to_addr]);
                Self {
                    socket_addr,
                    connect_to: Some(connect_to_addr),
                    listener,
                    peers_to_connect,
                    connections: Default::default(),
                }
            },
            None => {
                let peers_to_connect = Default::default();
                Self {
                    socket_addr,
                    connect_to: None,
                    listener,
                    peers_to_connect,
                    connections: Default::default(),
                }
            }
        }
    }
}

impl Actor for Peer {
    type Context = Context<Self>;

    /// Start peer with --connect flag: trying establish connection
    fn started(&mut self, ctx: &mut Context<Self>) {
        if self.connect_to.is_some() {
            ctx.address()
                .send(ConnectAddr(self.connect_to.unwrap()))
                .into_actor(self)
                .map(|res, _act, ctx| match res {
                    Ok(stream) => {
                        println!("TcpClientActor connected!");
                    }
                    Err(err) => {
                        println!("TcpClientActor failed to connected: {}", err);
                        ctx.stop();
                    }
                })
                // .map_err(|err, _act, ctx| {
                //     println!("TcpClientActor failed to connected: {}", err);
                //     ctx.stop();
                // })
                .spawn(ctx)
                //.wait(ctx);
        } else {
            info!("Peer is the first peer");
        }

    }

    fn stopped(&mut self, ctx: &mut Context<Self>) {
        println!("Actor is stopped");
    }
}

#[derive(Message)]
#[rtype(result = "Responses")]
pub enum Messages {
    RandomMessage,
    PeersRequest,
}

pub enum Responses {
    // For now keep it here
    GotRandomMessage,
    PeersResponse(HashSet<SocketAddr>),
}

impl Handler<ConnectAddr> for Peer {
    type Result = ResponseActFuture<Self, Result<TcpStream, ResolverError>>;

    fn handle(&mut self, msg: ConnectAddr, ctx: &mut Self::Context) -> Self::Result {
        let mut v = VecDeque::new();
        v.push_back(msg.0);
        Box::new(TcpConnector::new(v))
    }
}

impl Handler<Messages> for Peer {
    type Result = Responses;

    fn handle(&mut self, msg: Messages, _ctx: &mut Context<Self>) -> Self::Result {
        let peers = Default::default();
        match msg {
            Messages::RandomMessage => Responses::GotRandomMessage,
            Messages::PeersRequest => Responses::PeersResponse(peers),
        }
    }
}

impl<A, M> MessageResponse<A, M> for Responses
    where
        A: Actor,
        M: Message<Result = Responses>,
{
    fn handle(self, ctx: &mut A::Context, tx: Option<OneshotSender<M::Result>>) {
        if let Some(tx) = tx {
            let sent = tx.send(self);
        }
    }
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct RandomMessage(pub String);

#[derive(Message, Debug)]
#[rtype(result = "Peers")]
pub struct PeersRequest;

pub struct Peers(pub HashSet<SocketAddr>);

impl Display for RandomMessage {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "([{}])", self.0)
    }
}

impl Handler<RandomMessage> for Peer {
    type Result = ();

    fn handle(&mut self, msg: RandomMessage, _ctx: &mut Context<Self>) -> Self::Result {
        info!("random message: {msg}");
        ()
    }
}



impl Handler<PeersRequest> for Peer {
    type Result = MessageResult<PeersRequest>;

    fn handle(&mut self, _msg: PeersRequest, ctx: &mut Self::Context) -> Self::Result {

        todo!()
    }
}
