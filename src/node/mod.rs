use crate::datastore::error::DatastoreError;
use crate::datastore::example_datastore::ExampleDatastore;
use crate::datastore::tx_data::TxResult;
use crate::datastore::{self, *};
use crate::durability::omnipaxos_durability::OmniPaxosDurability;
use crate::durability::{DurabilityLayer, DurabilityLevel};
use omnipaxos::{messages::*, OmniPaxos};
use omnipaxos::util::{LogEntry, NodeId};
use std::borrow::Borrow;
use std::collections::HashMap;
use std::ops::Index;
use std::sync::{Arc, Mutex};
use tokio::{sync::mpsc, time};
use crate::durability::omnipaxos_durability::OmniLogEntry;

// New use
use std::time::Duration;

pub const OUTGOING_MESSAGE_PERIOD: Duration = Duration::from_millis(2);
pub const TICK_PERIOD: Duration = Duration::from_millis(10);

pub struct NodeRunner {
    pub node: Arc<Mutex<Node>>,
    // TODO Messaging and running
    pub incoming: mpsc::Receiver<Message<OmniLogEntry>>,
    pub outgoing: HashMap<NodeId, mpsc::Sender<Message<OmniLogEntry>>>,
}

impl NodeRunner {
    async fn send_outgoing_msgs(&mut self) {
        let all_outgoing_omnipaxos_messages = self
            .node
            .lock()
            .unwrap()
            .omnipaxos_durability
            .omni_paxos
            .outgoing_messages();
        for message in all_outgoing_omnipaxos_messages{
            let receiver = message.get_receiver();
            let channel = self
                .outgoing
                .get_mut(&receiver)
                .expect("There is no channel for the receiver");
            let _ = channel.send(message).await;
        }
    }


    pub async fn run(&mut self) {
        let mut outgoing_interval = time::interval(OUTGOING_MESSAGE_PERIOD);
        let mut tick_interval = time::interval(TICK_PERIOD);
        loop {
            tokio::select! {
                biased;
                _ = tick_interval.tick() => {
                    self
                    .node
                    .lock()
                    .unwrap()
                    .omnipaxos_durability
                    .omni_paxos
                    .tick();
                    
                    // We consider to add a sleep inside here 

                    self
                    .node
                    .lock()
                    .unwrap()
                    .update_leader();
                },
                _ = outgoing_interval.tick() => {
                    self
                    .send_outgoing_msgs().await;
                },
                Some(in_message) = self.incoming.recv() => {
                    self
                    .node
                    .lock()
                    .unwrap()
                    .omnipaxos_durability
                    .omni_paxos
                    .handle_incoming(in_message);
                },
                else => {}
            }
        }
    }
}

pub struct Node {
    node_id: NodeId, // Unique identifier for the node
                     // TODO Datastore and OmniPaxosDurability
    omnipaxos_durability: OmniPaxosDurability,
    datastore: datastore::example_datastore::ExampleDatastore,
    omnipaxos_cluster_leader: NodeId,
}

impl Node {
    pub fn new(node_id: NodeId, omni_durability: OmniPaxosDurability) -> Self {
        todo!()
    }

    /// update who is the current leader. If a follower becomes the leader,
    /// it needs to apply any unapplied txns to its datastore.
    /// If a node loses leadership, it needs to rollback the txns committed in
    /// memory that have not been replicated yet.
    pub fn update_leader(&mut self){
                
        let leader = match self.omnipaxos_durability.omni_paxos.get_current_leader() {
            Some(leader) => leader,
            None => panic!("No leader elected")
        };

        if self.omnipaxos_cluster_leader != leader{
            if self.omnipaxos_cluster_leader == self.node_id{
                self.datastore.rollback_to_replicated_durability_offset().expect("There is nothing to roll back");
                
                match self.omnipaxos_durability.omni_paxos
                .trim(match self.datastore.get_replicated_offset() {
                    Some(index) => Some(index.0),
                    None => panic!("There is no replicated offset")
                }) {
                    Ok(_) => println!("Trimmed successfully!"),
                    Err(_) => panic!("Couldn't trim any more!")
                }
            }else if self.node_id == leader {
                let  tx_box = self
                .omnipaxos_durability
                .iter_starting_from_offset(
                    match self.datastore.get_replicated_offset(){
                    Some(offset) => offset,
                    None => panic!("There is no offset")
                });
                
                for  (tx_offset, tx_data) in tx_box {
                    self.omnipaxos_durability.append_tx(tx_offset, tx_data);
                }

            }
            self.omnipaxos_cluster_leader = leader;    
        }
        
    }
    
    /// Apply the transactions that have been decided in OmniPaxos to the Datastore.
    /// We need to be careful with which nodes should do this according to desired
    /// behavior in the Datastore as defined by the application.
    fn apply_replicated_txns(&mut self) {
        let iter = self
        .omnipaxos_durability
        .omni_paxos
        .read_entries(0..self.omnipaxos_durability.omni_paxos.get_compacted_idx());
        
    }

    pub fn begin_tx(
        &self,
        durability_level: DurabilityLevel,
    ) -> <ExampleDatastore as Datastore<String, String>>::Tx {
        todo!()
    }

    pub fn release_tx(&self, tx: <ExampleDatastore as Datastore<String, String>>::Tx) {
        todo!()
    }

    /// Begins a mutable transaction. Only the leader is allowed to do so.
    pub fn begin_mut_tx(
        &self,
    ) -> Result<<ExampleDatastore as Datastore<String, String>>::MutTx, DatastoreError> {
        todo!()
    }

    /// Commits a mutable transaction. Only the leader is allowed to do so.
    pub fn commit_mut_tx(
        &mut self,
        tx: <ExampleDatastore as Datastore<String, String>>::MutTx,
    ) -> Result<TxResult, DatastoreError> {
        todo!()
    }

    fn advance_replicated_durability_offset(
        &self,
    ) -> Result<(), crate::datastore::error::DatastoreError> {
        todo!()
    }
}

/// Your test cases should spawn up multiple nodes in tokio and cover the following:
/// 1. Find the leader and commit a transaction. Show that the transaction is really *chosen* (according to our definition in Paxos) among the nodes.
/// 2. Find the leader and commit a transaction. Kill the leader and show that another node will be elected and that the replicated state is still correct.
/// 3. Find the leader and commit a transaction. Disconnect the leader from the other nodes and continue to commit transactions before the OmniPaxos election timeout.
/// Verify that the transaction was first committed in memory but later rolled back.
/// 4. Simulate the 3 partial connectivity scenarios from the OmniPaxos liveness lecture. Does the system recover? *NOTE* for this test you may need to modify the messaging logic.
///
/// A few helper functions to help structure your tests have been defined that you are welcome to use.
#[cfg(test)]
mod tests {
    use crate::node::*;
    use omnipaxos::messages::Message;
    use omnipaxos::util::NodeId;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};
    use tokio::runtime::{Builder, Runtime};
    use tokio::sync::mpsc;
    use tokio::task::JoinHandle;

    const SERVERS: [NodeId; 3] = [1, 2, 3];

    #[allow(clippy::type_complexity)]
    fn initialise_channels() -> (
        HashMap<NodeId, mpsc::Sender<Message<_>>>,
        HashMap<NodeId, mpsc::Receiver<Message<_>>>,
    ) {
        todo!()
    }

    fn create_runtime() -> Runtime {
        Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap()
    }

    fn spawn_nodes(runtime: &mut Runtime) -> HashMap<NodeId, (Arc<Mutex<Node>>, JoinHandle<()>)> {
        let mut nodes = HashMap::new();
        let (sender_channels, mut receiver_channels) = initialise_channels();
        for pid in SERVERS {
            todo!("spawn the nodes")
        }
        nodes
    }
}
