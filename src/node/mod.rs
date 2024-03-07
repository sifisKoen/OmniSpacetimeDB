use crate::datastore::error::DatastoreError;
use crate::datastore::example_datastore::ExampleDatastore;
use crate::datastore::tx_data::TxResult;
use crate::datastore::*;
use crate::durability::omnipaxos_durability::{OmniLogEntry, OmniPaxosDurability};
use crate::durability::{DurabilityLayer, DurabilityLevel};


use tokio::time;

use omnipaxos::util::{LogEntry, NodeId};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc;
use omnipaxos::messages::Message;

use self::tx_data::TxData;



pub struct NodeRunner {
    pub node: Arc<Mutex<Node>>,
    // TODO Messaging and running

    pub incoming: mpsc::Receiver<Message<OmniLogEntry>>,
    pub outgoing: HashMap<NodeId, mpsc::Sender<Message<OmniLogEntry>>>,
}



type TxOffset = u64;


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
        const OUTGOING_MESSAGE_PERIOD: Duration = Duration::from_secs(1); // Replace `1` with the desired duration in seconds
        
        let mut outgoing_interval = time::interval(OUTGOING_MESSAGE_PERIOD);
        const TICK_PERIOD: Duration = Duration::from_secs(1); // Replace `1` with the desired duration in seconds
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
    datastore: example_datastore::ExampleDatastore,
    omnipaxos_cluster_leader: NodeId,
}

impl Node {
    pub fn new(node_id: NodeId, omni_durability: OmniPaxosDurability) -> Self {
        //todo!()
        Node{
            node_id,
            omnipaxos_durability:omni_durability,
            datastore: example_datastore::ExampleDatastore::new(),
            //leader_id:NodeId
            omnipaxos_cluster_leader: node_id,
    }
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
        //todo!()
        pub enum OmniLogEntry {
            Decided(DecidedEntry),
            // Add other variants if needed
        }

        pub struct DecidedEntry {
            pub tx_offset: TxOffset,
            pub tx_data: TxData,
        }

        // Rest of the code remains unchanged
        let mut iter = self.omnipaxos_durability.omni_paxos.read_entries(0..self.omnipaxos_durability.omni_paxos.get_decided_idx()).unwrap();

        let decided_entries: Vec<(u64, TxData)> = iter.iter().filter_map(|log_entry| {
            match log_entry {
                LogEntry::Decided(entry) => Some((entry.tx_offset.0, entry.tx_data.clone())),
                _ => None,
            }
        }).collect();
        for (offset, data) in decided_entries{
            let mut transaction = self.datastore.begin_mut_tx();
            transaction.set(offset.to_string(), format!("{:?}", data));
            let _ = self.datastore.commit_mut_tx(transaction);
        }  
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
        Ok(self.datastore.begin_mut_tx())
    }

    /// Commits a mutable transaction. Only the leader is allowed to do so.
    pub fn commit_mut_tx(
        &mut self,
        tx: <ExampleDatastore as Datastore<String, String>>::MutTx,
    ) -> Result<TxResult, DatastoreError> {
        //todo!()
        self.datastore.commit_mut_tx(tx)

    }

    fn advance_replicated_durability_offset(
        &self,
    ) -> Result<(), crate::datastore::error::DatastoreError> {
        //todo!()
        let result=self.datastore.get_replicated_offset();
        match result{
            Some(offset)=> self.datastore.advance_replicated_durability_offset(offset),
            None => Err(DatastoreError::default()),
        }
        
    }
}



/// Your test cases should spawn up multiple nodes in tokio and cover the following:
/// 1. Find the leader and commit a transaction. Show that the transaction is really *chosen* (according to our definition in Paxos) among the nodes.
/// 2. Find the leader and commit a transaction. Kill the leader and show that another node will be elected and that the replicated state is still correct.
/// 3. Find the leader and commit a transaction. Disconnect the leader from the other nodes and continue to commit transactions before the OmniPaxos election timeout.
/// Verify that the transaction was first committed in memory but later rolled back.
/// 4. Simulate the 3 partial connectivity scenarios from the OmniPaxos liveness lecture. Does the system recover? *NOTE* for this test you may need to modify the messaging logic.
/// A few helper functions to help structure your tests have been defined that you are welcome to use.



#[cfg(test)]
mod tests {
    use super::*;
    use crate::durability::omnipaxos_durability;
    use crate::node::*;
    use omnipaxos::messages::Message;
    use omnipaxos::util::NodeId;
    use omnipaxos::OmniPaxosConfig;
    use omnipaxos_storage::memory_storage::MemoryStorage;
    use tempdir::TempDir;
    use tests::tx_data::InsertList;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};
    use tokio::runtime::{Runtime, create_runtime};
    use tokio::sync::mpsc;
    use tokio::task::JoinHandle;
use omnipaxos::OmniPaxos;
use crate::durability::omnipaxos_durability::{update_leader}; // Add this line




#[test]


 fn test_commit_transactions_1() {
    fn spawn_nodes(runtime: &mut Runtime) -> HashMap<u64, (Arc<Mutex<Node>>, JoinHandle<()>)> {
        // Implementation of the `spawn_nodes` function goes here
        unimplemented!()
    }

    const WAIT_DECIDED_TIMEOUT: u64 = 1000; // Replace 1000 with the desired timeout value

    let mut runtime = create_runtime();
    let nodes = spawn_nodes(&mut runtime);
    // wait for leader to be elected...
    std::thread::sleep(WAIT_DECIDED_TIMEOUT * 2);
    let (first_server, _) = nodes.get(&1).unwrap();

    // check which server is the current leader

    let leader = first_server
        .lock()
        .unwrap()
        .omni_paxos_durability.omnipaxos
        .get_current_leader()
        .expect("Failed to get leader");

    let (leader_server, _leader_join_handle) = nodes.get(&leader).unwrap();
    //add a mutable transaction to the leader
    let mut tx = leader_server.lock().unwrap().begin_mut_tx().unwrap();
    tx.set("foo".to_string(), "bar".to_string());
    let result = leader_server.lock().unwrap().commit_mut_tx(tx).unwrap();
    leader_server.lock().unwrap().omni_paxos_durability.append_tx(result.tx_offset, result.tx_data);

    // wait for the entries to be decided...
    println!("Trasaction committed");

    std::thread::sleep(WAIT_DECIDED_TIMEOUT * 6);
    let last_replicated_tx = leader_server
        .lock()
        .unwrap()
        .begin_tx(DurabilityLevel::Replicated);
    
    // check that the transaction was replicated in leader
    assert_eq!(
        last_replicated_tx.get(&"foo".to_string()),
        Some("bar".to_string())
    );
    leader_server.lock().unwrap().release_tx(last_replicated_tx);
    // check that the transaction was replicated in the followers
    let follower = (leader + 1) as u64 % nodes.len() as u64;
    let (follower_server, _) = nodes.get(&follower).unwrap();
    let last_replicated_tx = follower_server
        .lock()
        .unwrap()
        .begin_tx(DurabilityLevel::Replicated);
    assert_eq!(
        last_replicated_tx.get(&"foo".to_string()),
        Some("bar".to_string())
    );

    follower_server
        .lock()
        .unwrap()
        .release_tx(last_replicated_tx);

    let mut tx = leader_server.lock().unwrap().begin_mut_tx().unwrap();
    tx.set("red".to_string(), "blue".to_string());
    let result: TxResult = leader_server.lock().unwrap().commit_mut_tx(tx).unwrap();
    leader_server.lock().unwrap().0.omni_paxos_durability.append_tx(result.tx_offset, result.tx_data);

    // wait for the entries to be decided...
    println!("Trasaction committed");
    std::thread::sleep(WAIT_DECIDED_TIMEOUT * 6);
    let last_replicated_tx: example_datastore::Tx = leader_server
        .lock()
        .unwrap()
        .begin_tx(DurabilityLevel::Replicated);
    let oof = leader_server.lock().unwrap().0.datastore.get_replicated_offset();
    
    // check that the transaction was replicated in leader
    assert_eq!(
        last_replicated_tx.get(&"red".to_string()),
        Some("blue".to_string())
    );
    leader_server.lock().unwrap().release_tx(last_replicated_tx);

    let last_replicated_tx = follower_server
        .lock()
        .unwrap()
        .begin_tx(DurabilityLevel::Replicated);
    // check that the transaction was replicated in leader
    assert_eq!(
        last_replicated_tx.get(&"red".to_string()),
        Some("blue".to_string())
    );
    leader_server.lock().unwrap().release_tx(last_replicated_tx);
    runtime.shutdown_background();

}
}