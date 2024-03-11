mod peer;
mod remote_peer;
mod codec;

use std::io;
use std::io::{Error, ErrorKind};
use std::net::SocketAddr;
use std::str::FromStr;
use std::time::Duration;
use clap::Parser;
use tracing::{error, Level};
use tracing::info;
use actix::prelude::*;
use crate::peer::Peer;


//#[actix_rt::main]
fn main() -> io::Result<()> {
    let subscriber = tracing_subscriber::fmt().with_max_level(Level::DEBUG).finish();
    tracing::subscriber::set_global_default(subscriber).expect("Could not set tracing subscriber");
    let args = Args::parse();
    let period =  args.period;
    let port = args.port;
    let connect = args.connect;

    let sys = System::new();

    sys.block_on( async {
        match connect {
            Some(connect_to) => {
                let socket_addr = format!("127.0.0.1:{}", connect_to);
                let socket_addr = SocketAddr::from_str(&socket_addr);
                if let Ok(addr) = socket_addr {
                    Peer::new(port, Duration::from_secs(period), Some(addr)).start();
                } else {
                    // TODO exit
                    error!("wrong peer addr");
                }
            },
            None => {
                Peer::new(port, Duration::from_secs(period), None).start();
            }
        }
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

//     // let peer;
//     // match connect {
//     //     Some(connect_to) => {
//     //         let addr = SocketAddr::from_str(&connect_to.to_string())
//     //             .map_err(|e| Error::new(ErrorKind::InvalidInput, e))?;
//     //         peer = Peer::new(port, Duration::from_secs(period), Some(addr)).start();
//     //     },
//     //     None => {
//     //         peer = Peer::new(port, Duration::from_secs(period), None).start();
//     //     }
//     // }