# Simple p2p gossiping app

The peer have a cli interface to start it and connect itself to the other peers. 
Once connected, the peer starts sending a random gossip message (and printing it) to all the other peers every N seconds. 
The messaging period [N] is specifiable in the command line. 
When a peer receives a message from the other peers, it prints it in the console.

* to build peer bin:

   `cargo build --release`

* to run peer app:

  `cargo run --package p2p_gossiping_app --bin p2p_gossiping_app -- [options]` or `target/release/p2p_gossiping_app [options]`

### Starting the first peer with messaging period 5 seconds at port 8080:

`cargo run --package p2p_gossiping_app --bin p2p_gossiping_app -- --period=5 --port=8080`

### Starting the second peer (with options) which will connect to the first, messaging period - 6 seconds, port - 8081

`cargo run --package p2p_gossiping_app --bin p2p_gossiping_app -- --period=6 --port=8081 --connect="127.0.0.1:8080"`

### Starting the third peer (with options) which will connect to all the peers through the first, messaging period - 7 seconds, port - 8082

`cargo run --package p2p_gossiping_app --bin p2p_gossiping_app -- --period=7 --port=8082 --connect="127.0.0.1:8080"`
