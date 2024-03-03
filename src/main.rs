pub mod datastore;
pub mod durability;
pub mod node;



use std::time::Duration;
use std::{collections::HashMap, error::Error, sync::{Arc, Mutex}};
use durability::omnipaxos_durability::OmniLogEntry;
use durability::omnipaxos_durability::OmniPaxosDurability;
use node::Node;
use node::NodeRunner;
use omnipaxos::{messages::Message, util::NodeId, ClusterConfig, ServerConfig};
use omnipaxos_storage::memory_storage::MemoryStorage;
//use parking_lot::Mutex;
use tokio::{runtime::{self, Builder}, sync::mpsc,join};

use crate::durability::omnipaxos_durability;

pub const ELECTION_TICK_TIMEOUT: u64 = 5;
pub const WAIT_LEADER_TIMEOUT: Duration = Duration::from_millis(500);
pub const BUFFER_SIZE: usize = 10000;

const SERVERS: [NodeId; 3]=[1,2,3];



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

#[tokio::main]
async fn main()   {
    let runtime = Builder::new_multi_thread()
    .worker_threads(4)
    .enable_all()
    .build()
    .unwrap();

    let configuration_id = 1;
    let mut omnipaxos_server_handles =HashMap::new();
    let(sender_channels, mut receiver_channels) = initialise_channels_between_nodes();
    
    for pid in SERVERS{
        let server_config = ServerConfig{
            pid,
            election_tick_timeout: ELECTION_TICK_TIMEOUT,
            ..Default::default()
        };
        let cluster_config = ClusterConfig{
            configuration_id,
            nodes: SERVERS.into(),
            ..Default::default()
        };
        let op_config = omnipaxos::OmniPaxosConfig{
            server_config,
            cluster_config,
        };
        //omnipaxos node that is used as a temporary variable to create a new instance of OmniPaxosDurability
        let omnipaxos_node: omnipaxos::OmniPaxos<OmniLogEntry, MemoryStorage<OmniLogEntry>>= op_config.build(MemoryStorage::default()).unwrap();
        //omnipaxos durability that is used as a temporary variable to create a new instance of Node
        let omnipaxos_durability= OmniPaxosDurability::new(omnipaxos_node);
        
        let mut node: Node = Node::new(pid, omnipaxos_durability);

        let mut node_runner: NodeRunner = NodeRunner {
            node: Arc::new(Mutex::new(node)),
            incoming: receiver_channels.remove(&pid).unwrap(),
            outgoing: sender_channels.clone(),
        };
        let join_handle=runtime.spawn({
            async move {
                node_runner.run().await;
            }
        });
        omnipaxos_server_handles.insert(pid, (node, join_handle));


    };

    
    join!(omnipaxos_server_handles.get(&1).unwrap().1, omnipaxos_server_handles.get(&2).unwrap().1, omnipaxos_server_handles.get(&3).unwrap().1);


    
    

    //Ok(())
}
