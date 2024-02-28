use super::*;

/*
Do we need to implement the OmniPaxosConfig, ServerConfig, 
and ClusterConfig in this file or into Node?
*/
use omnipaxos::{
    storage, utils, ClusterConfig, OmniPaxos, OmniPaxosConfig, ServerConfig
};

use omnipaxos_storage::memory_storage::MemoryStorage;

use omnipaxos::macros::Entry;
use omnipaxos::util::LogEntry as utilsLogEntry;

#[derive(Clone, Debug, Entry)]
// Represents an entry in the transaction log.
struct LogEntry { 
    tx_offset: TxOffset, // Transaction offset (key)
    tx_data: TxData, // Transaction data (value)
}

/// OmniPaxosDurability is a OmniPaxos node that should provide the replicated
/// implementation of the DurabilityLayer trait required by the Datastore.

pub struct OmniPaxosDurability {
    // TODO
    omni_paxos: OmniPaxos<LogEntry, MemoryStorage<LogEntry>>,
}

// Implement OmniPaxosDurability
impl OmniPaxosDurability {
    // Create a new instance of the durability sub-node in
    pub fn new(omnipaxos_cluster_config: OmniPaxosConfig) -> Self {
    /*
    We pass omnipaxos_cluster_config type of OmniPaxosConfig.
    We take this config from our Node.
    */
        let storage = MemoryStorage::default();
        let omni_paxos: OmniPaxos::<LogEntry, MemoryStorage<LogEntry>> = omnipaxos_cluster_config.build(storage).unwrap();

        OmniPaxosDurability{
            omni_paxos,
        }
    }
}

impl DurabilityLayer for OmniPaxosDurability {

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

    fn append_tx(&mut self, tx_offset: TxOffset, tx_data: TxData) {
        let write_entry = LogEntry { tx_offset, tx_data};

        self.omni_paxos
            .append(write_entry)
            .expect("Failed to append entry")
    }

    fn get_durable_tx_offset(&self) -> TxOffset {
        TxOffset(self.omni_paxos.get_decided_idx())
    }
}

#[cfg(test)]
mod tests{
    use super::LogEntry;
}