use crate::datastore::{self, Datastore};

use super::*;

use omnipaxos::{
    OmniPaxos, OmniPaxosConfig, macros::Entry, util::LogEntry as utilsLogEntry
};

use omnipaxos_storage::memory_storage::MemoryStorage;

#[derive(Clone, Debug, Entry)]
// Represents an entry in the transaction log.
pub struct OmniLogEntry { 
    pub tx_offset: TxOffset, // Transaction offset (key)
    pub tx_data: TxData, // Transaction data (value)
}

/// OmniPaxosDurability is a OmniPaxos node that should provide the replicated
/// implementation of the DurabilityLayer trait required by the Datastore.
pub struct OmniPaxosDurability {
    pub omni_paxos: OmniPaxos<OmniLogEntry, MemoryStorage<OmniLogEntry>>,
}

// Implement OmniPaxosDurability
impl OmniPaxosDurability {
    // Create a new instance of the durability sub-node in
    pub fn new(omnipaxos_node: OmniPaxos<OmniLogEntry, MemoryStorage<OmniLogEntry>>) -> Self {
    /*
    We pass omnipaxos_cluster_config type of OmniPaxosConfig.
    We take this config from our Node.
    */
        let storage:MemoryStorage<OmniLogEntry> = MemoryStorage::default();
        let omni_paxos: OmniPaxos::<OmniLogEntry, MemoryStorage<OmniLogEntry>> = omnipaxos_node;

        OmniPaxosDurability{
            omni_paxos,
        }
    }
}

impl DurabilityLayer for OmniPaxosDurability {
    //this returns an iterator for the decided transactions that starts from the start of the log
    fn iter(&self) -> Box<dyn Iterator<Item = (TxOffset, TxData)>> {
        let log_iter = self.omni_paxos.read_entries(0..self.omni_paxos.get_decided_idx());
        let decided_entries: Vec<(TxOffset, TxData)> = log_iter.unwrap().iter().filter_map(|log_entry| {
            match log_entry {
                utilsLogEntry::Decided(entry) => Some((entry.tx_offset.clone(), entry.tx_data.clone())),
                _ => None,
            }
        }).collect();

        Box::new(decided_entries.into_iter())
    }
    //this returns an iterator for the decided transactions that starts from the offset that we pass
    fn iter_starting_from_offset(
        &self,
        offset: TxOffset,
    ) -> Box<dyn Iterator<Item = (TxOffset, TxData)>> {
        /*
        We start the iteration to our omni_paxos entries from the offset that we pass,
        until the decided index.
         */
        let log_iter = self.omni_paxos.read_entries(offset.0..self.omni_paxos.get_decided_idx());
        let decided_entries: Vec<(TxOffset, TxData)> = log_iter.unwrap().iter().filter_map(|log_entry| {
            match log_entry {
                utilsLogEntry::Decided(entry) => Some((entry.tx_offset.clone(), entry.tx_data.clone())),
                _ => None,
            }
        }).collect();

        Box::new(decided_entries.into_iter())
    }
    // this appends the transaction to the log
    fn append_tx(&mut self, tx_offset: TxOffset, tx_data: TxData) {
        let write_entry = OmniLogEntry { tx_offset, tx_data};

        self.omni_paxos
            .append(write_entry)
            .expect("Failed to append entry")
    }
    // this reurns the index of the last decided entry
    fn get_durable_tx_offset(&self) -> TxOffset {
        TxOffset(self.omni_paxos.get_decided_idx())
    }
}