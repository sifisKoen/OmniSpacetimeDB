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

pub struct NodeRunner {
    pub node: Arc<Mutex<Node>>,
    pub incoming: mpsc::Receiver<Message<OmniLogEntry>>,
    pub outgoing: HashMap<NodeId, mpsc::Sender<Message<OmniLogEntry>>>,
}

impl NodeRunner {
    async fn send_outgoing_msgs(&mut self) {
        let all_outgoing_omnipaxos_messages = self
            .node
            .lock()
            .unwrap()
            .durability
            .omni_paxos
            .outgoing_messages();
        for message in all_outgoing_omnipaxos_messages {
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
                    self.node.lock().unwrap().durability.omni_paxos.tick();
                    self.node.lock().unwrap().update_leader();
                },
                _ = outgoing_interval.tick() => {
                    self.send_outgoing_msgs().await;
                },
                Some(in_message) = self.incoming.recv() => {
                    self.node
                        .lock()
                        .unwrap()
                        .durability
                        .omni_paxos
                        .handle_incoming(in_message);
                },
                else => {}
            }
        }
    }
}

pub struct Node {
    node_id: NodeId,
    durability: OmniPaxosDurability,
    datastore: ExampleDatastore,
    omnipaxos_cluster_leader: NodeId,
}

impl Node {
    pub fn new(node_id: NodeId, omni_durability: OmniPaxosDurability) -> Self {
        Node {
            node_id,
            durability: omni_durability,
            datastore: ExampleDatastore::new(),
            omnipaxos_cluster_leader: node_id,
        }
    }

    pub fn update_leader(&mut self) {
        let leader = match self.durability.omni_paxos.get_current_leader() {
            Some(leader) => leader,
            None => panic!("No leader elected"),
        };

        if self.omnipaxos_cluster_leader != leader {
            if self.omnipaxos_cluster_leader == self.node_id {
                self.datastore
                    .rollback_to_replicated_durability_offset()
                    .expect("There is nothing to roll back");

                match self
                    .durability
                    .omni_paxos
                    .trim(match self.datastore.get_replicated_offset() {
                        Some(index) => Some(index.0),
                        None => panic!("There is no replicated offset"),
                    })
                {
                    Ok(_) => println!("Trimmed successfully!"),
                    Err(_) => panic!("Couldn't trim any more!"),
                }
            } else if self.node_id == leader {
                let tx_box = self.durability.iter_starting_from_offset(match self.datastore.get_replicated_offset() {
                    Some(offset) => offset,
                    None => panic!("There is no offset"),
                });

                for (tx_offset, tx_data) in tx_box {
                    self.durability.append_tx(tx_offset, tx_data);
                }
            }
            self.omnipaxos_cluster_leader = leader;
        }
    }

    fn apply_replicated_txns(&mut self) {
        let mut iter = self
            .durability
            .omni_paxos
            .read_entries(0..self.durability.omni_paxos.get_decided_idx())
            .unwrap();

        let decided_entries: Vec<(u64, TxData)> = iter
            .iter()
            .filter_map(|log_entry| match log_entry {
                LogEntry::Decided(entry) => Some((entry.tx_offset.0, entry.tx_data.clone())),
                _ => None,
            })
            .collect();

        for (offset, data) in decided_entries {
            let mut transaction = self.datastore.begin_mut_tx();
            transaction.set(offset.to_string(), format!("{:?}", data));
            let _ = self.datastore.commit_mut_tx(transaction);
        }
    }

    pub fn begin_tx(
        &self,
        durability_level: DurabilityLevel,
    ) -> <ExampleDatastore as Datastore<String, String>>::Tx {
        self.datastore.begin_tx(durability_level)
    }

    pub fn release_tx(&self, tx: <ExampleDatastore as Datastore<String, String>>::Tx) {
        self.datastore.release_tx(tx)
    }

    pub fn begin_mut_tx(
        &self,
    ) -> Result<<ExampleDatastore as Datastore<String, String>>::MutTx, DatastoreError> {
        Ok(self.datastore.begin_mut_tx())
    }

    pub fn commit_mut_tx(
        &mut self,
        tx: <ExampleDatastore as Datastore<String, String>>::MutTx,
    ) -> Result<TxResult, DatastoreError> {
        self.datastore.commit_mut_tx(tx)
    }

    fn advance_replicated_durability_offset(
        &self,
    ) -> Result<(), crate::datastore::error::DatastoreError> {
        let result = self.datastore.get_replicated_offset();
        match result {
            Some(offset) => self.datastore.advance_replicated_durability_offset(offset),
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
///
/// A few helper functions to help structure your tests have been defined that you are welcome to use.
#[cfg(test)]
mod tests {
    use crate::{durability, node::*};
    use omnipaxos::messages::Message;
    use omnipaxos::{ClusterConfig, ServerConfig};
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};
    use tokio::runtime::{Builder, Runtime};
    use tokio::sync::mpsc;
    use tokio::task::JoinHandle;
    use self::durability::omnipaxos_durability::Log;

    const SERVERS: [NodeId; 5] = [1, 2, 3, 4, 5];

    #[allow(clippy::type_complexity)]
    fn initialise_channels(server_number: u64) -> (
        HashMap<NodeId, mpsc::Sender<Message<Log>>>,
        HashMap<NodeId, mpsc::Receiver<Message<Log>>>,
    ) {
        let mut sender_channels = HashMap::new();
        let mut receiver_channels = HashMap::new();
        if server_number == 0 {
            for pid in SERVERS {
                let (sender, receiver) = mpsc::channel(100);
                sender_channels.insert(pid, sender);
                receiver_channels.insert(pid, receiver);
            }
        }
        else {
            for k in 1..server_number+1 {
                let (sender, receiver) = mpsc::channel(100);
                sender_channels.insert(k, sender);
                receiver_channels.insert(k, receiver);
            }
        }
        for pid in SERVERS {
            let (sender, receiver) = mpsc::channel(100);
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

    fn spawn_nodes(runtime: &mut Runtime, node_number:u64) -> HashMap<NodeId, (Arc<Mutex<Node>>, JoinHandle<()>)> {
        let mut nodes = HashMap::new();
        let (sender_channels, mut receiver_channels) = initialise_channels(node_number);
        if node_number == 0 {
            for pid in SERVERS {
                // todo!("spawn the nodes")
                let server_config = ServerConfig{
                    pid,
                    election_tick_timeout: ELECTION_TICK_TIMEOUT,
                    ..Default::default()
                };
                let cluster_config = ClusterConfig{
                    configuration_id: 1,
                    nodes: SERVERS.into(),
                    ..Default::default()
                };
                let durability = OmniPaxosDurability::new(server_config.clone(), cluster_config.clone()).unwrap();
                let node: Arc<Mutex<Node>> = Arc::new(Mutex::new(Node::new(pid, durability, SERVERS.len() as u64)));
                let mut node_runner = NodeRunner {
                    node: node.clone(),
                    incoming: receiver_channels.remove(&pid).unwrap(),
                    outgoing: sender_channels.clone(),
                };
                let join_handle = runtime.spawn({
                    async move {
                        node_runner.run().await;
                    }
                });
                nodes.insert(pid, ( node, join_handle));
            }
        }
        else {
            for pid in 1..node_number+1 {
                // todo!("spawn the nodes")
                let server_config = ServerConfig{
                    pid,
                    election_tick_timeout: ELECTION_TICK_TIMEOUT,
                    ..Default::default()
                };
                let vec: Vec<u64> = (1..=node_number).collect();
                let cluster_config = ClusterConfig{
                    configuration_id: 1,
                    nodes: vec,
                    ..Default::default()
                };
                let durability = OmniPaxosDurability::new(server_config.clone(), cluster_config.clone()).unwrap();
                let node: Arc<Mutex<Node>> = Arc::new(Mutex::new(Node::new(pid, durability, node_number as u64)));
                let mut node_runner = NodeRunner {
                    node: node.clone(),
                    incoming: receiver_channels.remove(&pid).unwrap(),
                    outgoing: sender_channels.clone(),
                };
                let join_handle = runtime.spawn({
                    async move {
                        node_runner.run().await;
                    }
                });
                nodes.insert(pid, ( node, join_handle));
            }
        }
        nodes
    }

    #[test]
    fn basic_test_cluster_size() {
        let mut runtime = create_runtime();
        let nodes = spawn_nodes(&mut runtime,0);
        std::thread::sleep(WAIT_LEADER_TIMEOUT);
        assert_eq!(SERVERS.len(), nodes.len());
    }

    #[test]
    //TestCase #1 Find the leader and commit a transaction. Show that the transaction is really *chosen* (according to our definition in Paxos) among the nodes
    fn test_case_1() {
        let mut runtime = create_runtime();
        let nodes = spawn_nodes(&mut runtime, 0);
        std::thread::sleep(WAIT_LEADER_TIMEOUT);
        let (first_server, _) = nodes.get(&1).unwrap();
        let leader_pid = first_server
             .lock()
             .unwrap()
             .durability.omni_paxos
             .get_current_leader()
             .expect("Failed to get leader");

        let leader = nodes.get(&leader_pid).unwrap();
        let mut tx = leader.0.lock().unwrap().begin_mut_tx().unwrap();
        leader.0.lock().unwrap().datastore.set_mut_tx(&mut tx, "key1".to_string(), "value1".to_string());
        let transaction = leader.0.lock().unwrap().commit_mut_tx(tx).unwrap();
        leader.0.lock().unwrap().durability.append_tx(transaction.tx_offset, transaction.tx_data);
        // After committing the transaction, check the leader status
        std::thread::sleep(TICK_PERIOD);
        let leader_iter = leader.0.lock().unwrap().durability.iter();
        let leader_offset = leader.0.lock().unwrap().durability.get_durable_tx_offset();
        let leader_collected: Vec<_> = leader_iter.collect();
        //check that follower nodes are in sync with the leader
        for pid in SERVERS {
            let (server, _) = nodes.get(&pid).unwrap();
            if server.lock().unwrap().node_id != leader_pid {
                let iter = server.lock().unwrap().durability.iter();
                let collected: Vec<_> = iter.collect();
                assert_eq!(collected.len(), leader_collected.len());
                assert_eq!(leader_offset, server.lock().unwrap().durability.get_durable_tx_offset());
            }
        
        //Here we will assert that a node which is not a leader cannot commit
        }
    }
    

    #[test]
    /// 2. Find the leader and commit a transaction. Kill the leader and show that another node will be elected and that the replicated state is still correct.
    fn test_case_2() {
        let mut runtime = create_runtime();
        let nodes = spawn_nodes(&mut runtime, 0);
        std::thread::sleep(WAIT_LEADER_TIMEOUT);
        let (first_server, _) = nodes.get(&1).unwrap();
        let leader_pid = first_server
             .lock()
             .unwrap()
             .durability.omni_paxos
             .get_current_leader()
             .expect("Failed to get leader");
        
         println!("Elected leader: {}", leader_pid);
         println!("Current Offset: {}", first_server.lock().unwrap().durability.get_durable_tx_offset().0);

        let leader = nodes.get(&leader_pid).unwrap();
        let mut tx = leader.0.lock().unwrap().begin_mut_tx().unwrap();
        leader.0.lock().unwrap().datastore.set_mut_tx(&mut tx, "key1".to_string(), "value1".to_string());
        let transaction = leader.0.lock().unwrap().commit_mut_tx(tx).unwrap();
        leader.0.lock().unwrap().durability.append_tx(transaction.tx_offset, transaction.tx_data);
        // After committing the transaction, check the leader status
        std::thread::sleep(TICK_PERIOD);
        let leader_iter = leader.0.lock().unwrap().durability.iter();
        let leader_offset = leader.0.lock().unwrap().durability.get_durable_tx_offset();
        let leader_collected: Vec<_> = leader_iter.collect();
        leader.1.abort();
        std::thread::sleep(WAIT_LEADER_TIMEOUT);
        let (second, _) = nodes.get(&2).unwrap();
        let new_leader = second
             .lock()
             .unwrap()
             .durability.omni_paxos
             .get_current_leader()
             .expect("Failed to get leader");
        assert_ne!(leader_pid, new_leader);
        println!("New leader: {}", new_leader);
        let new_leader_offset = second.lock().unwrap().durability.get_durable_tx_offset();
        let new_leader_iter = second.lock().unwrap().durability.iter();
        let new_leader_collected: Vec<_> = new_leader_iter.collect();
        // print the collected 
        println!("Leader Collected: {:?}", leader_collected);
        println!("Leader Collected: {:?}", new_leader_collected);
        assert_eq!(leader_collected.len(), new_leader_collected.len());

    }

    /// 3. Find the leader and commit a transaction. Disconnect the leader from the other nodes and continue to commit transactions before the OmniPaxos election timeout.
    /// Verify that the transaction was first committed in memory but later rolled back.
    #[test]
// 3. Find the leader and commit a transaction. Disconnect the leader from the other nodes and continue to commit transactions before the OmniPaxos election timeout.
// Verify that the transaction was first committed in memory but later rolled back.
 fn test_case_3() {
     let mut runtime = create_runtime();
     let nodes = spawn_nodes(&mut runtime, 0);
     std::thread::sleep(WAIT_LEADER_TIMEOUT);
     let (first_server, _) = nodes.get(&1).unwrap();
     let leader_pid = first_server
         .lock()
         .unwrap()
         .durability
         .omni_paxos
         .get_current_leader()
         .expect("Failed to get leader");
     println!("Elected leader: {}", leader_pid);
     println!(
         "Current Offset: {}",
         first_server.lock().unwrap().durability.get_durable_tx_offset().0
     );
     // Assuming you have already obtained the leader_pid and nodes HashMap
 let leader = nodes.get(&leader_pid).unwrap();
 for _ in 0..5 {
     let mut tx = leader.0.lock().unwrap().begin_mut_tx().unwrap();
     leader
         .0
         .lock()
         .unwrap()
         .datastore
         .set_mut_tx(&mut tx, "key1".to_string(), "value1".to_string());
     let transaction = leader
         .0
         .lock()
         .unwrap()
         .commit_mut_tx(tx)
         .unwrap();
     leader.0.lock().unwrap().durability.append_tx(transaction.tx_offset, transaction.tx_data);
 }
 println!(
     "Current Offset before cutting connection, should be 0+5=5: {}",
     leader.0.lock().unwrap().datastore.get_cur_offset().unwrap().0
 );
     std::thread::sleep(WAIT_LEADER_TIMEOUT);
     println!("Replicated logs: {:?}", leader.0.lock().unwrap().durability.iter().collect::<Vec<_>>());
     println!(
         "test, should be 0+5=5: {}",
         leader.0.lock().unwrap().datastore.get_cur_offset().unwrap().0
     );
     println!(
         "test, should be 0+5=5: {}",
         leader.0.lock().unwrap().datastore.get_cur_offset().unwrap().0
     );
     println!(
         "Current durability offset, should be 0+5=5: {}",
         leader.0.lock().unwrap().durability.get_durable_tx_offset().0
     );
     println!(
         "Current durability datastore offset, should be 0+5=5: {}",
         leader.0.lock().unwrap().datastore.get_replicated_offset().unwrap().0
     );
     // Cutting the connection
     leader.0.lock().unwrap().messaging_allowed = false;
     // Adding some commits
     for _ in 0..5 {
         let mut tx = leader.0.lock().unwrap().begin_mut_tx().unwrap();
         leader
             .0
             .lock()
             .unwrap()
             .datastore
             .set_mut_tx(&mut tx, "key1".to_string(), "value1".to_string());
         let transaction = leader
             .0
             .lock()
             .unwrap()
             .commit_mut_tx(tx)
             .unwrap();
     }
     let value2 = leader.0.lock().unwrap().datastore.get_cur_offset().unwrap().0.to_le();
     println!(
         "Current Offset after cutting connection, should be 5+5=10: {}",
         value2
     );
     std::thread::sleep(WAIT_LEADER_TIMEOUT);
     println!(
         "test, should be 5+6=11: {}",
         leader.0.lock().unwrap().datastore.get_cur_offset().unwrap().0
     );
     // Simulate waiting for some time (more than WAIT_LEADER_TIMEOUT)
     std::thread::sleep(WAIT_LEADER_TIMEOUT * 2);
     // Verify that the transaction was rolled back after the timeout
     // Verify that the transaction was rolled back after the timeout
     let leader_first_check = leader
     .0
     .lock()
     .unwrap()
     .datastore
     .get_cur_offset()
     .unwrap()
     .0
     .to_le();
     let (s1, _) = nodes.get(&1).unwrap();
     let (s2, _) = nodes.get(&2).unwrap();
     let (s3, _) = nodes.get(&3).unwrap();
     println!("Current length of the iter is {:?} after timeout", leader_first_check);
     println!("New leader, AFTER TIMEOUT, for 1: {}", s1.lock().unwrap().durability.omni_paxos.get_current_leader().unwrap());
     println!("New leader, AFTER TIMEOUT, for old leader: {}", s3.lock().unwrap().durability.omni_paxos.get_current_leader().unwrap());
     let length_after_timeout = leader_first_check;
     println!("Current length of the iter is {:?} after timeout", length_after_timeout);
     leader.0.lock().unwrap().messaging_allowed = true;
  
     //s1.lock().unwrap().durability.omni_paxos.reconnected(3);
     //s2.lock().unwrap().durability.omni_paxos.reconnected(3);
     std::thread::sleep(WAIT_LEADER_TIMEOUT * 2);
     println!("New leader, AFTER REJOIN, for 1: {}", s1.lock().unwrap().durability.omni_paxos.get_current_leader().unwrap());
     println!("New leader, AFTER REJOIN, for old leader: {}", s3.lock().unwrap().durability.omni_paxos.get_current_leader().unwrap());
     let length_after_rejoin = s3.lock().unwrap().datastore.get_cur_offset().unwrap().0.to_le();
     println!("Current length of the iter is {:?} after rejoin", length_after_rejoin);
     //s3.lock().unwrap().datastore.rollback_to_replicated_durability_offset().unwrap();
     //s3.lock().unwrap().update_leader();
     let length_after_rejoin = s3.lock().unwrap().datastore.get_cur_offset().unwrap().0.to_le();
     println!("Current length of the iter is {:?} after forced rollback", length_after_rejoin);
     // Assert that the second commit length is equal to the first check length
     //assert_eq!(leader_second_commit_len, leader_first_check);
 }


    #[test]
    /// 4. Simulate the 3 partial connectivity scenarios from the OmniPaxos liveness lecture. Does the system recover? *NOTE* for this test you may need to modify the messaging logic.
    /// First Scenario Quorum-Loss Scenario
    fn test_case_4_loss() {
        let mut runtime = create_runtime();
        let nodes = spawn_nodes(&mut runtime,0);
        std::thread::sleep(WAIT_LEADER_TIMEOUT);
        let (first_server, _) = nodes.get(&1).unwrap();
        let leader = first_server
            .lock()
            .unwrap()
            .durability.omni_paxos
            .get_current_leader()
            .expect("Failed to get leader");
        println!("Elected leader: {}", leader);
        let leader_to_be = 1 as u64;

        for pid in SERVERS {
            if pid != leader_to_be {
                let (server_to_get, _) = nodes.get(&pid).unwrap();
                for pid1 in SERVERS{
                    if pid1 != leader_to_be{
                        server_to_get.lock().unwrap().connected_nodes[pid1 as usize-1] = false;
                    }
                }
            }    
        }

        std::thread::sleep(WAIT_LEADER_TIMEOUT * 2);
        let new_leader = nodes.get(&leader_to_be).unwrap().0.lock().unwrap().durability.omni_paxos.get_current_leader().expect("Failed to get leader");
        println!("Newly elected leader {:?}", new_leader);

    }






    #[test]
    /// 4. Simulate the 3 partial connectivity scenarios from the OmniPaxos liveness lecture. Does the system recover? *NOTE* for this test you may need to modify the messaging logic.
    /// Constrained-Election Scenario
    fn test_case_4_constrained() {
        let mut runtime = create_runtime();
        let nodes = spawn_nodes(&mut runtime,0);
        std::thread::sleep(WAIT_LEADER_TIMEOUT);
        let (first_server, _) = nodes.get(&1).unwrap();
        
        //Get the leader
        let leader = first_server
            .lock()
            .unwrap()
            .durability.omni_paxos
            .get_current_leader()
            .expect("Failed to get leader");
        println!("Elected leader: {}", leader);


        //Step 1
        //For all the servers that is not the leader and it should only be connected to a common server that is not yet a leader
        for pid in SERVERS {
            if pid != leader {
                let (server, _) = nodes.get(&pid).unwrap();
                server.lock().unwrap().connected_nodes[leader as usize-1] = false;
            }
        }

        //Give some time
        std::thread::sleep(WAIT_LEADER_TIMEOUT * 2);

        //Step 2
        //Isolate the leader
        let (server, _) = nodes.get(&leader).unwrap();
        server.lock().unwrap().messaging_allowed = false;

        //Give some time for the effect to take place
        std::thread::sleep(WAIT_LEADER_TIMEOUT * 2);

        //Print the leader for the isolated old leader
        let isolated_leader = server.lock().unwrap().durability.omni_paxos.get_current_leader().expect("Failed to get leader");
        println!("Leader for the old Isolated leader: {}", isolated_leader);

        //Step 3
        //Get the new leader from a node that is not the leader
        let new_leader = nodes.get(&2).unwrap().0.lock().unwrap().durability.omni_paxos.get_current_leader().expect("Failed to get leader");
        println!("New Elected leader: {}", new_leader);
        
    }
    #[test]
    /// 4. Simulate the 3 partial connectivity scenarios from the OmniPaxos liveness lecture. Does the system recover? *NOTE* for this test you may need to modify the messaging logic.
    /// Chained Scenario 
    fn test_case_4_chained() {
        let mut runtime = create_runtime();
        let nodes = spawn_nodes(&mut runtime,3);
        std::thread::sleep(WAIT_LEADER_TIMEOUT);
        let (server1, _) = nodes.get(&1).unwrap();
        let leader_pid = server1
            .lock()
            .unwrap()
            .durability
            .omni_paxos
            .get_current_leader()
            .expect("Failed to get leader");
        println!("Elected leader: {}", leader_pid);
        let mut server2 = nodes.get(&2).unwrap().0.lock().unwrap();
        let mut leader = nodes.get(&leader_pid).unwrap().0.lock().unwrap();
        server2.connected_nodes[3-1] = false;
        leader.connected_nodes[2-1] = false;
        println!("Leader connections: {:?}", leader.connected_nodes);
        println!("Server2 connections: {:?}", server2.connected_nodes);
        std::thread::sleep(WAIT_LEADER_TIMEOUT*2);
        let new_leader = server2.durability.omni_paxos.get_current_leader().expect("Failed to get leader");
        println!("Newly elected leader {:?}", new_leader);
        std::thread::sleep(WAIT_LEADER_TIMEOUT*2);
        let new_leader = server2.durability.omni_paxos.get_current_leader().expect("Failed to get leader");
        println!("Newly elected leader after waiting again {:?}", new_leader);
        // Currently not the behaviour we want, because 2 does not become leader, but at least it does not leads to constant reelections
    }




}