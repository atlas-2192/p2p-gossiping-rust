use std::io;
use std::time::Duration;
use clap::Parser;
use tracing::Level;
use tracing::info;
use actix::prelude::*;
use crate::peer::{Peer, SendRandomMessage};

mod peer;
mod error;
mod network;
mod codec;
mod actor;

fn main() -> io::Result<()> {
    let subscriber = tracing_subscriber::fmt().with_max_level(Level::DEBUG).finish();
    tracing::subscriber::set_global_default(subscriber).expect("Could not set tracing subscriber");
    let args = Args::parse();
    let period =  args.period;
    let port = args.port;
    let connect = args.connect;

    let mut sys = System::new();

    sys.block_on( async {
        let peer;
        match connect {
            Some(connect_to) => {
                peer = Peer::new(Duration::from_secs(period), port, Some(connect_to));
            },
            None => {
                peer = Peer::new(Duration::from_secs(period), port, None);
            }
        }
        peer.await.expect("REASON").start()
    });

    let _ = sys.run();
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
