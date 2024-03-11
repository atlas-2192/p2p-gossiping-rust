use std::net::SocketAddr;
use actix::{Actor, Context, Handler, MessageResult};
use crate::codec::{Request, Response};

/// Remote peer representation
pub struct RemotePeer {
    pub socket_addr: SocketAddr,
}

impl Actor for RemotePeer {
    type Context = Context<Self>;
}

impl Handler<Request> for RemotePeer {
    type Result = MessageResult<Response>;

    fn handle(&mut self, msg: Request, ctx: &mut Self::Context) -> Self::Result {
        todo!()
    }
}