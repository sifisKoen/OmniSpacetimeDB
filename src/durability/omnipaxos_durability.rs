use self::example_durability::ExampleDurabilityLayer;
use super::*;
use crate::datastore::{tx_data::TxData, TxOffset};
use omnipaxos::{
    OmniPaxos, OmniPaxosConfig, macros::Entry, util::LogEntry as UtilsLogEntry, errors::ConfigError
};
use omnipaxos_storage::memory_storage::MemoryStorage;

#[derive(Clone, Debug, Entry)]
pub struct OmniLogEntry {
    pub tx_offset: TxOffset,
    pub tx_data: TxData,
}

pub struct OmniPaxosDurability {
    pub omni_paxos: OmniPaxos<OmniLogEntry, MemoryStorage<OmniLogEntry>>,
}

impl OmniPaxosDurability {
    pub fn new(omni_paxos: OmniPaxos<OmniLogEntry, MemoryStorage<OmniLogEntry>>) -> Self {
        let storage = MemoryStorage::default();

        OmniPaxosDurability { omni_paxos }
    }
}

impl DurabilityLayer for OmniPaxosDurability {
    fn iter(&self) -> Box<dyn Iterator<Item = (TxOffset, TxData)>> {
        if let Some(entries) = &self.omni_paxos.read_entries(..) {
            let decided_entries: Vec<_> = entries.iter().filter_map(|log_entry| {
                match log_entry {
                    UtilsLogEntry::Decided(entry) | UtilsLogEntry::Undecided(entry) => {
                        Some((entry.tx_offset.clone(), entry.tx_data.clone()))
                    }
                    _ => None,
                }
            }).collect();

            Box::new(decided_entries.into_iter())
        } else {
            Box::new(std::iter::empty())
        }
    }

    fn iter_starting_from_offset(&self, offset: TxOffset) -> Box<dyn Iterator<Item = (TxOffset, TxData)>> {
        if let Some(entries) = &self.omni_paxos.read_entries(..) {
            let decided_entries: Vec<_> = entries.iter().filter_map(|log_entry| {
                match log_entry {
                    UtilsLogEntry::Decided(entry) | UtilsLogEntry::Undecided(entry) => {
                        if entry.tx_offset >= offset {
                            Some((entry.tx_offset.clone(), entry.tx_data.clone()))
                        } else {
                            None
                        }
                    }
                    _ => None,
                }
            }).collect();

            Box::new(decided_entries.into_iter())
        } else {
            Box::new(std::iter::empty())
        }
    }

    fn append_tx(&mut self, tx_offset: TxOffset, tx_data: TxData) {
        let write_entry = OmniLogEntry { tx_offset, tx_data };

        if let Err(err) = self.omni_paxos.append(write_entry) {
            eprintln!("Error appending entry to OmniPaxos: {:?}", err);
        }
    }

    fn get_durable_tx_offset(&self) -> TxOffset {
        TxOffset(self.omni_paxos.get_decided_idx())
    }
}
