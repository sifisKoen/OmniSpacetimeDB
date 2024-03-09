use super::*;
use omnipaxos_storage::memory_storage::MemoryStorage;
use crate::datastore::{tx_data::TxData, TxOffset};
use omnipaxos::{
    errors::ConfigError,
    macros::Entry,
    util::LogEntry,
    OmniPaxos,
    ClusterConfig,
    ServerConfig,
};

// Definition of the Log structure representing a log entry
#[derive(Debug, Clone, Entry)]
pub struct TransactionLog {
    tx_offset: TxOffset,
    tx_data: TxData,
}

// Definition of the OmniPaxosDurability struct
pub struct OmniPaxosDurability {
    // A field to store the OmniPaxos instance with Log entries and MemoryStorage
    pub omni_paxos: OmniPaxos<TransactionLog, MemoryStorage<TransactionLog>>,
}

// Implementation of methods for OmniPaxosDurability
impl OmniPaxosDurability {
    // Constructor for OmniPaxosDurability
    pub fn new(server_config: ServerConfig, cluster_config: ClusterConfig) -> Result<OmniPaxosDurability, ConfigError> {
        // Building the cluster configuration for the OmniPaxos instance
        let cluster_config = cluster_config.build_for_server::<TransactionLog, MemoryStorage<TransactionLog>>(
            server_config,
            MemoryStorage::default(),
        );

        // Handling the result of building the OmniPaxos instance
        match cluster_config {
            Ok(cluster_config) => {
                println!("OmniPaxos instance created successfully.");
                // Returning the OmniPaxosDurability instance with the created OmniPaxos
                Ok(OmniPaxosDurability { omni_paxos: cluster_config })
            }
            Err(err) => {
                println!("Error creating OmniPaxos instance: {:?}", err);
                // Returning the error if OmniPaxos instance creation fails
                Err(err)
            }
        }
        
    }
}

// Implementation of the DurabilityLayer trait for OmniPaxosDurability
impl DurabilityLayer for OmniPaxosDurability {
    // Implementation of the iter method to iterate over all entries
    fn iter(&self) -> Box<dyn Iterator<Item = (TxOffset, TxData)>> {
        // Checking if entries are available in the OmniPaxos instance
        if let Some(entries) = &self.omni_paxos.read_entries(..) {
            // Creating an iterator over the entries and flattening the results
            let entry_iter = entries.iter().flat_map(|entry| match entry {
                // Extracting TxOffset and TxData from Decided and Undecided entries
                LogEntry::Decided(log) | LogEntry::Undecided(log) => {
                    Some((log.tx_offset.clone(), log.tx_data.clone()))
                }
                // Ignoring other types of entries
                _ => None,
            });

            // Returning the iterator as a Box
            Box::new(entry_iter.collect::<Vec<_>>().into_iter())
        } else {
            // Returning an empty iterator if no entries are available
            Box::new(std::iter::empty())
        }
    }

    // Implementation of iter_starting_from_offset to iterate from a specific offset
    fn iter_starting_from_offset(&self, offset: TxOffset) -> Box<dyn Iterator<Item = (TxOffset, TxData)>> {
        // Checking if entries are available in the OmniPaxos instance
        if let Some(entries) = &self.omni_paxos.read_entries(..) {
            // Creating an iterator over the entries and filtering by offset
            let entry_iter = entries.iter().filter_map(|entry| match entry {
                // Extracting TxOffset and TxData from Decided and Undecided entries with offset check
                LogEntry::Decided(log) | LogEntry::Undecided(log) => {
                    if log.tx_offset >= offset {
                        Some((log.tx_offset.clone(), log.tx_data.clone()))
                    } else {
                        None
                    }
                }
                // Ignoring other types of entries
                _ => None,
            });

            // Returning the iterator as a Box
            Box::new(entry_iter.collect::<Vec<_>>().into_iter())
        } else {
            // Returning an empty iterator if no entries are available
            Box::new(std::iter::empty())
        }
    }

    // Implementation of append_tx to add a new transaction entry
    fn append_tx(&mut self, tx_offset: TxOffset, tx_data: TxData) {
        // Creating a Log entry from the provided TxOffset and TxData
        let log = TransactionLog {
            tx_offset,
            tx_data,
        };

        // Appending the Log entry to the OmniPaxos instance
        match self.omni_paxos.append(log) {
            Ok(_) => println!("Entry appended to OmniPaxos."),
            Err(e) => println!("Error appending entry to OmniPaxos: {:?}", e),
        }
    }

    // Implementation of get_durable_tx_offset to get the durable transaction offset
    fn get_durable_tx_offset(&self) -> TxOffset {
        // Getting the decided index from the OmniPaxos instance and returning as TxOffset
        TxOffset(self.omni_paxos.get_decided_idx())
    }
}
