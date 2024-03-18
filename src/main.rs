#![feature(buf_read_has_data_left)]
mod peer;
mod codec;
mod connection;
pub(crate) mod message;

use std::io;
use std::net::SocketAddr;
use std::str::FromStr;
use std::time::Duration;
use clap::Parser;
use tracing::{error, Level};
use actix::prelude::*;
use time::format_description;
use time::macros::format_description;
use tracing_subscriber::fmt;
use crate::peer::Peer;


fn main() -> io::Result<()> {
    format_description!("[hour]:[minute]:[second]");
    let timer = format_description::parse("[hour]:[minute]:[second]").unwrap();
    let time_offset =
        time::UtcOffset::current_local_offset().unwrap_or_else(|_| time::UtcOffset::UTC);
    let timer = fmt::time::OffsetTime::new(time_offset, timer);

    let subscriber = tracing_subscriber::fmt()
        .with_timer(timer)
        .with_target(false)
        .with_max_level(Level::DEBUG)
        .finish();

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