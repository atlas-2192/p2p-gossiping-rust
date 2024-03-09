use std::net::SocketAddr;
use std::time::Duration;
use actix::{Actor, ActorContext, ActorFutureExt, ActorTryFutureExt, AsyncContext, Context, fut, Handler, Message, MessageResult, Running, StreamHandler, WrapFuture};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tracing::{debug, error, info, warn};
use uuid::Uuid;
use crate::gen_rnd_msg;
use crate::network::{read_message, send_message};
use crate::peer::{Connection, Peer};

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
#[rtype(result = "()")]
pub(crate) struct WriteOutgoingMessages {
    conn_id: Uuid,
    peer_addr: SocketAddr,
    write: OwnedWriteHalf,
    period: Duration,
}

impl StreamHandler<NewConnection> for Peer {
    fn handle(&mut self, new_conn: NewConnection, ctx: &mut Self::Context) {
        let peer_addr = new_conn.0.peer_addr().unwrap();
        info!("new connection is being handled: {:?}", peer_addr);
        let conn = crate::peer::Connection::new(peer_addr);
        let conn_id = conn.id.clone();
        if self.peers_to_connect.insert(conn).is_err() {
            warn!("Couldn't add peer to connections as it's been already added")
        }

        let (read, write) = new_conn.0.into_split();
        // start handling incoming connections
        ctx.notify(ReadIncomingMessages {
            conn_id,
            peer_addr,
            read
        });
        // start sending messages
        ctx.notify(WriteOutgoingMessages {
            conn_id,
            peer_addr,
            write,
            period: self.period
        });
    }
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

impl Handler<WriteOutgoingMessages> for Peer {
    type Result = ();

    fn handle(&mut self, mut msg: WriteOutgoingMessages, ctx: &mut Self::Context) -> Self::Result {
        let period = self.period;
        ctx.spawn(async move {
            // TODO check 'static
            let rnd_msg = gen_rnd_msg();
            loop {
                tokio::time::sleep(period).await;
                let rnd_msg = rnd_msg.clone();
                info!("sending message [{rnd_msg}] to [{}]", msg.peer_addr);
                if let Err(e) = send_message(&mut msg.write, &rnd_msg).await {
                    // TODO handle errors
                    error!("Couldn't send message: {rnd_msg}, error: {e}");
                    break
                }
            }
        }
                      .into_actor(self)
                  //.then(|actor| {})
        );
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
            ctx.spawn(async move {
                TcpStream::connect(connect_to).await
            }
                .into_actor(self)
                .map_err(move |err, _actor, ctx| {
                    error!("couldn't establish connection with peer: {}", err);
                    // TODO
                    //ctx.stop()
                })
                .then(move |stream, actor, ctx| {
                    debug!("connection has been established: {:?}", stream);
                    let stream = stream.unwrap();
                    let peer_addr = stream.peer_addr().unwrap();
                    let write = stream.into_split().1;
                    let conn = Connection::new(peer_addr);
                    //actor.peer_conns.insert(conn.id.clone(), conn);
                    ctx.notify(WriteOutgoingMessages {
                        conn_id: conn.id.clone(),
                        peer_addr,
                        write,
                        period
                    });
                    fut::ready(())
                })
            );
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

#[derive(Message)]
#[rtype(result = "bool")]
/// Handshake process emulation
pub(crate) struct TryHandshake(SocketAddr);

impl Handler<TryHandshake> for Peer {
    type Result = bool;

    fn handle(&mut self, msg: TryHandshake, ctx: &mut Self::Context) -> Self::Result {
        debug!("trying to perform handshake with peer [{}]", msg.0);

        false
    }
}

#[derive(Message, Debug)]
#[rtype(result = "Peers")]
pub(crate) struct PeersRequest;

#[derive(Debug)]
pub(crate) struct Peers(pub std::collections::HashSet<SocketAddr>);

impl Handler<PeersRequest> for Peer {
    type Result = MessageResult<PeersRequest>;

    fn handle(&mut self, _msg: PeersRequest, _ctx: &mut Self::Context) -> Self::Result {
        debug!("handle PeersRequest...");
        let mut peers = std::collections::HashSet::new();
        &self.peers_to_connect.scan(|p| { peers.insert(p.peer_addr); () });
        MessageResult(Peers(peers))
    }
}