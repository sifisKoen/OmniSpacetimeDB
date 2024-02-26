use super::*;

use omnipaxos::*;
use omnipaxos_storage::*;
use std::{
    fs::File,
    io::{Read, Write, Seek, SeekFrom},
    sync::{Arc, Mutex},
};
use tokio::{
    fs::{File as AsyncFile, OpenOptions as AsyncOpenOptions},
    io::AsyncWriteExt,
    sync::mpsc,
};

use crate::datastore::{tx_data::TxData, TxOffset};

struct LogEntry {
    tx_offset: TxOffset,
    tx_data: TxData,
}

impl LogEntry {
    fn serialize<W: Write>(&self, writer: &mut W) -> std::io::Result<()> {
        writer.write_all(&self.tx_offset.0.to_le_bytes())?;
        self.tx_data.serialize(writer)
    }

    fn deserialize<R: Read>(reader: &mut R) -> std::io::Result<Self> {
        let mut bytes = [0; 8];
        reader.read_exact(&mut bytes)?;
        let tx_offset = TxOffset(u64::from_le_bytes(bytes));
        let tx_data = TxData::deserialize(reader)?;
        Ok(LogEntry { tx_offset, tx_data })
    }
}

/// OmniPaxosDurability is a OmniPaxos node that should provide the replicated
/// implementation of the DurabilityLayer trait required by the Datastore.
#[derive(Clone)]
pub struct OmniPaxosDurability {
    tx: Option<mpsc::Sender<LogEntry>>,
    durable_offset: Arc<Mutex<Option<TxOffset>>>,
    log_file: Arc<Mutex<Option<AsyncFile>>>,
    filepath: String,
    replica_id: u64,
    replica_offset: Arc<Mutex<Option<TxOffset>>>

}

impl OmniPaxosDurability {
    pub fn new(filepath: &str, replica_id: u64) -> Self {
        OmniPaxosDurability {
            tx: None,
            durable_offset: Arc::new(Mutex::new(None)),
            log_file: Arc::new(Mutex::new(None)),
            filepath: filepath.to_string(),
            replica_id,
            replica_offset: Arc::new(Mutex::new(None))
        }
    }

    pub async fn open_log_file(filepath: &str) -> AsyncFile {
        AsyncOpenOptions::new()
            .write(true)
            .create(true)
            .open(filepath)
            .await
            .expect("Failed to open log file")
    }

    pub fn close_log_file(&mut self) {
        // Close the log file.
        self.log_file = Arc::new(Mutex::new(None));
    }

    async fn file_write_task(
        mut rx: mpsc::Receiver<LogEntry>,
        out_tx: mpsc::Sender<TxOffset>,
        filepath: String,
    ) {
        let mut file = AsyncFile::create(&filepath)
            .await
            .expect("Failed to open file");

        while let Some(request) = rx.recv().await {
            // Serialize your data to a Vec<u8> as needed
            let mut src = Vec::new();
            // Assuming you have a serialize method
            request
                .serialize(&mut src)
                .expect("Failed to serialize data");

            // Perform the write operation
            file.write_all(&src).await.expect("Failed to write to file");
            file.flush().await.expect("Failed to flush file");
            file.sync_all().await.expect("Failed to sync file");
            out_tx
                .send(request.tx_offset)
                .await
                .expect("Failed to send write request");
        }
    }
}

impl DurabilityLayer for OmniPaxosDurability {
    fn iter(&self) -> Box<dyn Iterator<Item = (TxOffset, TxData)>> {
        let durable_offset = self.durable_offset.lock().unwrap();
        let mut offset = durable_offset.as_ref().unwrap().0;

        // Open the log file for reading.
        let mut file = File::open(&self.filepath).expect("Failed to open log file");


        Box::new(std::iter::from_fn(move || {
            match LogEntry::deserialize(&mut file) {
                Ok(entry) => {
                    let tx_offset = TxOffset(offset);
                    offset += 1;
                    Some((tx_offset, entry.tx_data))
                }
                Err(_) => None,
            }
        }))
    }

    fn iter_starting_from_offset(
        &self,
        offset: TxOffset,
    ) -> Box<dyn Iterator<Item = (TxOffset, TxData)>> {
        let durable_offset = self.durable_offset.lock().unwrap();
        let mut offset = durable_offset.as_ref().unwrap().0;

        // Open the log file for reading.
        let mut file = File::open(&self.filepath).expect("Failed to open log file");

        // Move the cursor to the desired offset.
        file.seek(SeekFrom::Start(offset.0)).expect("Failed to seek to offset");


        Box::new(std::iter::from_fn(move || {
            match LogEntry::deserialize(&mut file) {
                Ok(entry) => {
                    let tx_offset = TxOffset(offset);
                    offset += 1;
                    Some((tx_offset, entry.tx_data))
                }
                Err(_) => None,
            }
        }))
    }

    fn append_tx(&mut self, tx_offset: TxOffset, tx_data: TxData) {
        // This is where the durability layer would append the transaction to the log.
        // For the omnipaxos durability layer, we will be writing the TxData to an append only file.

        if self.tx.is_none() {
            let (tx, rx) = mpsc::channel::<LogEntry>(64);
            let (out_tx, mut out_rx) = mpsc::channel::<TxOffset>(64);
            let filepath = self.filepath.clone();
            let replica_id = self.replica_id;
            tokio::spawn(Self::file_write_task(rx, out_tx, filepath));
            let durable_offset = self.durable_offset.clone();
            let replica_offset = self.replica_offset.clone();
            tokio::spawn(async move {
                while let Some(offset) = out_rx.recv().await {
                    let mut durable_offset = durable_offset.lock().unwrap();
                    *durable_offset = Some(offset);
                    let mut replica_offset = replica_offset.lock().unwrap();
                    *replica_offset = Some(offset);
                }
            });
            self.tx = Some(tx);
        }

        let tx = self.tx.as_ref().cloned().unwrap();
        tokio::spawn(async move {
            tx.send(LogEntry { tx_offset, tx_data })
                .await
                .expect("Failed to send write request");
        });
    }

    fn get_durable_tx_offset(&self) -> TxOffset {
        let mut durable_offset = self.durable_offset.lock().unwrap();
        if let Some(offset) = durable_offset.as_ref() {
            // TODO: It's silly to read the whole log to get this number
            return *offset;
        }
        let offset = self
            .iter()
            .last()
            .map(|(tx_offset, _)| tx_offset)
            .unwrap_or(TxOffset(0));
        *durable_offset = Some(offset);
        offset
    }
}
