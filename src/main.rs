use std::io;
use std::time::Duration;
use clap::Parser;
use tracing::Level;
use tracing::info;
use actix::prelude::*;
use crate::peer::{Peer, RandomMessage};

mod peer;
mod error;
mod network;

//#[tokio::main]
#[actix_rt::main]
async fn main() -> io::Result<()> {
    let subscriber = tracing_subscriber::fmt().with_max_level(Level::DEBUG).finish();
    tracing::subscriber::set_global_default(subscriber).expect("Could not set tracing subscriber");
    let args = Args::parse();
    let period =  args.period;
    let port = args.port;
    let connect = args.connect;
    // start new actor
    let addr;
    match connect {
        Some(connect_to) => {
            addr = Peer::new(8080, Some(connect_to)).await.start();
        },
        None => {
            addr = Peer::new(8080, None).await.start();
        }
    }

    // send message and get future for result
    let res = addr.send(RandomMessage(gen_rnd_msg())).await;

    // handle() returns tokio handle
    //info!("RESULT: {}", res.unwrap());

    // stop system and exit
    System::current().stop();
    Ok(())
}

/// Command line args
#[derive(Parser, Debug)]
#[command(about, long_about = None)]
struct Args {
    /// send a random gossip message to all the other peers every N seconds
    #[arg(long)]
    period: u64,
    /// Port to start peer on
    #[arg(long)]
    port: u32,
    /// The 'connect_to' arg is None if this peer is first in the network
    #[arg(long)]
    connect: Option<u32>,
}

fn gen_rnd_msg() -> String {
    use random_word::Lang;
    let msg = random_word::gen(Lang::En);
    String::from(msg)
}
