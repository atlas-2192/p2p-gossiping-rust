// use std::net::SocketAddr;
// use actix::{Actor, Context, Handler, MessageResult, StreamHandler};
// use tokio::io::WriteHalf;
// use tokio::net::TcpStream;
// use tracing::info;
// use crate::codec::{Peers, RemotePeerConnectionCodec, Request, Response};
// 
// /// Remote peer representation
// pub struct RemotePeer {
//     pub socket_addr: SocketAddr,
//     pub write: actix::io::FramedWrite<Response, WriteHalf<TcpStream>, RemotePeerConnectionCodec>,
// }
// 
// impl RemotePeer {
//     pub fn new(
//         socket_addr: SocketAddr, 
//         write: actix::io::FramedWrite<Response, WriteHalf<TcpStream>, RemotePeerConnectionCodec>) 
//         -> Self {
//         Self {
//             socket_addr,
//             write
//         }
//     }
// }
// 
// impl Actor for RemotePeer {
//     type Context = Context<Self>;
// }
// 
// impl StreamHandler<Request> for RemotePeer {
//     //type Result = MessageResult<Response>;
// 
//     fn handle(&mut self, msg: Request, ctx: &mut Self::Context)  { // -> Self::Result
//         match msg {
//             Request::RandomMessage(msg) => { info!("received message [{}] from []", msg.0); }
//             Request::PeersRequest(req) => {}
//         }
//     }
// }
// 
// impl Handler<Request> for RemotePeer {
//     type Result = Response;
// 
//     fn handle(&mut self, msg: Request, ctx: &mut Self::Context) -> Self::Result {
//         todo!()
//     }
// }