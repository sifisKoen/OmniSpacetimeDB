pub mod datastore;
pub mod durability;
pub mod node;

use std::{collections::HashMap, error::Error};
use durability::omnipaxos_durability::OmniLogEntry;
use omnipaxos::{messages::Message, util::NodeId};
use tokio::{runtime::{self, Builder}, sync::mpsc};

const SERVERS: [NodeId; 3]=[1,2,3];

pub const BUFFER_SIZE: usize = 10000;

#[allow(clippy::type_complexity)]
fn initialise_channels_between_nodes() -> (
    HashMap<NodeId,mpsc::Sender<Message<OmniLogEntry>>>,
    HashMap<NodeId, mpsc::Receiver<Message<OmniLogEntry>>>
){
    let mut sender_channels = HashMap::new();
    let mut receiver_channels = HashMap::new();
    for pid in SERVERS{//each process corresponds to a node
        let (sender, receiver) = mpsc::channel(BUFFER_SIZE);
        sender_channels.insert(pid, sender);
        receiver_channels.insert(pid, receiver);
    }

    (sender_channels,receiver_channels)
}

fn main() -> Result<(), Box<dyn Error>> {
    let runtime = Builder::new_multi_thread()
    .worker_threads(4)
    .enable_all()
    .build()
    .unwrap();

    
    
    //Ok(())
}
