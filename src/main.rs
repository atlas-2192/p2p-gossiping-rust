use clap::Parser;

mod peer;
mod sender;
mod receiver;

fn main() {
    let args = Args::parse();
    let period =  args.period;
    let port = args.port;
    let connect = args.connect;
    println!("My address is '127.0.0.1:{}'", &port);
}

/// Command line args
#[derive(Parser, Debug)]
#[command(about, long_about = None)]
struct Args {
    /// send a random gossip message to all the other peers every N seconds
    #[arg(long)]
    period: u32,
    /// Port to start peer on
    #[arg(long)]
    port: u32,
    /// The 'connect_to' arg is None if this peer is first in the network
    #[arg(long)]
    connect: Option<String>,
}
