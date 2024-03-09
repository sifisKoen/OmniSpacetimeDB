use crate::datastore::error::DatastoreError;
use crate::datastore::example_datastore::ExampleDatastore;
use crate::datastore::tx_data::TxResult;
use crate::durability::omnipaxos_durability::OmniPaxosDurability;
use crate::durability::omnipaxos_durability::OmniLogEntry;
use crate::durability::{DurabilityLayer, DurabilityLevel};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use crate::datastore::{self, *};
use omnipaxos::{messages::*, OmniPaxos};
use omnipaxos::util::{LogEntry, NodeId};
use std::borrow::Borrow;
use tokio::{sync::mpsc, time};
use crate::datastore::*;
use crate::datastore::tx_data::*;
//use datastore::tx_data::TxOffset;




// New use
use std::time::Duration;

pub const OUTGOING_MESSAGE_PERIOD: Duration = Duration::from_millis(2);
pub const TICK_PERIOD: Duration = Duration::from_millis(10);
pub const UPDATE_TX_PERIOD: Duration = Duration::from_millis(5);
pub const UPDATE_LEADER_PERIOD: Duration = Duration::from_millis(5);
pub const BUFFER_SIZE: usize = 10000;

pub struct NodeRunner {
    pub node: Arc<Mutex<Node>>,
    // TODO Messaging and running
    pub incoming: mpsc::Receiver<Message<OmniLogEntry>>,
    pub outgoing: HashMap<NodeId, mpsc::Sender<Message<OmniLogEntry>>>,
}

impl NodeRunner {
    async fn send_outgoing_msgs(&mut self) {
        //Here we send the outgoing messages to the other nodes
        //We get the outgoing messages from the omnipaxos node
        let all_outgoing_omnipaxos_messages = self
            .node
            .lock()
            .unwrap()
            .omnipaxos_durability
            .omni_paxos
            .outgoing_messages();
        //We iterate over the messages and send them to the other nodes
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
        let mut update_tx = time::interval(UPDATE_TX_PERIOD);
        let mut update_leader = time::interval(UPDATE_LEADER_PERIOD);
        //here we have an infinite loop in which the node enters when it starts running
        loop {
            //we run the following branches concurrently
            tokio::select! {
                biased;
                //we perform a tick on the omnipaxos node which helps in the election of a leader
                _ = tick_interval.tick() => {
                    self
                    .node
                    .lock()
                    .unwrap()
                    .omnipaxos_durability
                    .omni_paxos
                    .tick();
                },
                //we execute the update_leader function which performs updates on the node
                _= update_leader.tick() => {
                    self
                    .node
                    .lock()
                    .unwrap()
                    .update_leader();
                },
                //we send the outgoing messages to the other nodes
                _ = outgoing_interval.tick() => {
                    self
                    .send_outgoing_msgs().await;
                },
                //we apply the transactions that have been decided
                _ = update_tx.tick() => {
                    self
                    .node
                    .lock()
                    .unwrap()
                    .apply_replicated_txns();
                },
                //we receive the incoming messages from the other nodes
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
    //we have the omnipaxos durability which contains the omnipaxos node
    omnipaxos_durability: OmniPaxosDurability,
    //we have the datastore which contains the data that the node stores
    datastore: example_datastore::ExampleDatastore,
    // this is the id of the leader of the cluster
    omnipaxos_cluster_leader: NodeId,
    //this is the nodes in the network
    network_nodes: Vec<NodeId>,

    
}
//here we implement the functions for the Node 
impl Node {
    //this is the function that creates a new instance of the Node
    pub fn new(node_id: NodeId, omni_durability: OmniPaxosDurability,nodes: Vec<NodeId>) -> Self {
        //todo!()
        Node{
            node_id,
            omnipaxos_durability:omni_durability,
            datastore: example_datastore::ExampleDatastore::new(),
            omnipaxos_cluster_leader: node_id,
            network_nodes: nodes,

        }
    }

    /// update who is the current leader. If a follower becomes the leader,
    /// it needs to apply any unapplied txns to its datastore.
    /// If a node loses leadership, it needs to rollback the txns committed in
    /// memory that have not been replicated yet.
    pub fn update_leader(&mut self){
        //we get the current leader from the omnipaxos node    
        let leader = match self.omnipaxos_durability.omni_paxos.get_current_leader() {
            Some(leader) => leader,
            None => panic!("No leader elected")
        };

        if leader == self.node_id {
            //we apply the transactions that have been decided
            self.apply_replicated_txns();
        } else {
            //we rollback the transactions that have not been replicated
            self.rollback_not_replicated_txns();
        }

        //we update the leader id to the new leader
        self.omnipaxos_cluster_leader = leader;    
        
    }

    /// Apply the transactions that have been decided in OmniPaxos to the Datastore.
    /// We need to be careful with which nodes should do this according to desired
    /// behavior in the Datastore as defined by the application.
    fn apply_replicated_txns(&mut self) {
        //todo!()
        //we get the transactions that have been decided
        //not from 0 but from the last replicated one in the node
        let current_offset_option = self.datastore.get_replicated_offset();
        let current_offset = match current_offset_option{
            Some(offset) => offset,
            None => TxOffset(0),
        };

        //we get the transactions from omnipaxos
        let mut transactions_iter = self.omnipaxos_durability.iter_starting_from_offset(current_offset);

        //we apply the transactions to the datastore
        while let Some((offset, data)) = transactions_iter.next() {
            match self.datastore.replay_transaction(&data) {
                //if we got ok we continue
                Ok(_) => {}
                Err(error) => {
                    // If we fail to apply a transaction, we print it
                    println!("{}",error);
                }
            }
        }

        //we advance the offset to the last decided transaction
        self.advance_replicated_durability_offset().expect("There was an error when trying to advance the offset");

    }

    // Rollback transactions that were applied locally while we thought we were the leader
    fn rollback_not_replicated_txns(&mut self) {
        //we rollback the transactions that have not been replicated
        self.datastore.rollback_to_replicated_durability_offset().expect("There was an error when trying to roll back");
        self.advance_replicated_durability_offset().expect("There was an error when trying to change the offset");
    }
    
    pub fn begin_tx(
        &self,
        durability_level: DurabilityLevel,
    ) -> <ExampleDatastore as Datastore<String, String>>::Tx {
        //todo!()
        self.datastore.begin_tx(durability_level)
    }

    pub fn release_tx(&self, tx: <ExampleDatastore as Datastore<String, String>>::Tx) {
        //todo!()
        self.datastore.release_tx(tx)

    }

    /// Begins a mutable transaction. Only the leader is allowed to do so.
    pub fn begin_mut_tx(
        &self,
    ) -> Result<<ExampleDatastore as Datastore<String, String>>::MutTx, DatastoreError> {
        //todo!()
        //we get the current leader from the omnipaxos node
        let leader_option=self.omnipaxos_durability.omni_paxos.get_current_leader();
        match leader_option{
            Some(leader)=> {
                //if we are the leader we begin the transaction
                if leader==self.node_id{
                    Ok(self.datastore.begin_mut_tx())
                //if we are not the leader we return an error
                }else{
                    Err(DatastoreError::default())
                }
            },
            None => Err(DatastoreError::default()),
        }
    }

    /// Commits a mutable transaction. Only the leader is allowed to do so.
    pub fn commit_mut_tx(
        &mut self,
        tx: <ExampleDatastore as Datastore<String, String>>::MutTx,
    ) -> Result<TxResult, DatastoreError> {
        //todo!()
        //we get the current leader from the omnipaxos node
        let leader_option=self.omnipaxos_durability.omni_paxos.get_current_leader();
        match leader_option{
            Some(leader)=> {
                //if we are the leader we commit the transaction
                if leader==self.node_id{
                    self.datastore.commit_mut_tx(tx)
                //if we are not the leader we return an error
                }else{
                    Err(DatastoreError::default())
                }
            },
            None => Err(DatastoreError::default()),
        }
    }

    fn advance_replicated_durability_offset(
        &self,
    ) -> Result<(), crate::datastore::error::DatastoreError> {
        //todo!()
        // we get the decided index from omni
        let offset = self.omnipaxos_durability.omni_paxos.get_decided_idx();
        //we set the offset to the datastore
        self.datastore.advance_replicated_durability_offset(TxOffset(offset))
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
        HashMap<NodeId, mpsc::Sender<Message<OmniLogEntry>>>,
        HashMap<NodeId, mpsc::Receiver<Message<OmniLogEntry>>>,
    ) {

        let mut sender_channels = HashMap::new();
        let mut receiver_channels = HashMap::new();

        for pid in SERVERS {
            let (sender, receiver) = mpsc::channel(BUFFER_SIZE);
            sender_channels.insert(pid, sender);
            receiver_channels.insert(pid, receiver);
        }
        (sender_channels, receiver_channels)
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
