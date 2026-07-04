use std::io;
use std::marker::PhantomData;
use std::path::Path;
use std::sync::mpsc::{Receiver, SyncSender, TryRecvError, sync_channel};
use std::thread;

use openraft::alias::{LogIdOf, VoteOf};
use openraft::entry::RaftEntry;
use openraft::storage::{IOFlushed, RaftLogStorage};
use openraft::{LogState, OptionalSend, OptionalSync, RaftLogReader, RaftTypeConfig};

use seqlog::{Error, SeqLog, SeqLogReader, SeqLogSyncer};

// The store.
pub struct SeqLogStore<C>
where
    C: RaftTypeConfig,
    C::Entry: EntryCodec,
{
    store: SeqLog,
    ioflush_tx: SyncSender<IOFlushed<C>>,
    write_buf: Vec<u8>,
    write_pos: Vec<usize>,
    _p: PhantomData<C>,
}

// The reader.
pub struct Reader<C>
where
    C: RaftTypeConfig,
    C::Entry: EntryCodec,
{
    inner: SeqLogReader,
    _p: PhantomData<C>,
}

pub trait EntryCodec: Sized + Send + OptionalSync + 'static {
    type EncodeError;
    type DecodeError: Send + Sync + 'static + std::error::Error;

    fn encode(&self, buf: &mut Vec<u8>) -> Result<(), Self::EncodeError>;

    fn decode(data: &[u8]) -> Result<Self, Self::DecodeError>;
}

impl<C> RaftLogReader<C> for Reader<C>
where
    C: RaftTypeConfig,
    C::Entry: EntryCodec,
{
    async fn try_get_log_entries<
        RB: std::ops::RangeBounds<u64> + Clone + std::fmt::Debug + OptionalSend,
    >(
        &mut self,
        range: RB,
    ) -> Result<Vec<<C as RaftTypeConfig>::Entry>, io::Error> {
        let start = match range.start_bound() {
            std::ops::Bound::Included(index) => *index,
            std::ops::Bound::Excluded(index) => *index + 1,
            std::ops::Bound::Unbounded => {
                return Err(new_other_err("unsupport unbounded start index"));
            }
        };
        if self.inner.next_seq() != start {
            // This is slow.
            self.inner.reset(start).map_err(seq_log_err)?;
        }

        let end = match range.end_bound() {
            std::ops::Bound::Included(index) => *index + 1,
            std::ops::Bound::Excluded(index) => *index,
            std::ops::Bound::Unbounded => u64::MAX,
        };

        let mut res = Vec::new();
        for _ in start..end {
            let Some(buf) = self.inner.next().map_err(seq_log_err)? else {
                break;
            };
            let entry = C::Entry::decode(buf).map_err(new_other_err)?;
            res.push(entry);
        }
        Ok(res)
    }

    async fn read_vote(
        &mut self,
    ) -> Result<Option<openraft::type_config::alias::VoteOf<C>>, io::Error> {
        todo!()
    }
}

impl<C> RaftLogStorage<C> for SeqLogStore<C>
where
    C: RaftTypeConfig,
    C::Entry: EntryCodec,
{
    type LogReader = Reader<C>;

    /// Returns the last deleted log id and the last log id.
    ///
    /// The impl should **not** consider the applied log id in state machine.
    /// The returned `last_log_id` could be the log id of the last present log entry, or the
    /// `last_purged_log_id` if there is no entry at all.
    // NOTE: This can be made into sync, provided all state machines will use atomic read or the
    // like.
    async fn get_log_state(&mut self) -> Result<LogState<C>, io::Error> {
        todo!()
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        let inner = self.store.reader(self.store.sync_seq(), true).unwrap();
        Reader {
            inner,
            _p: PhantomData::default(),
        }
    }

    /// Save vote to storage.
    ///
    /// ### To ensure correctness:
    ///
    /// The vote must be persisted on disk before returning.
    async fn save_vote(&mut self, vote: &VoteOf<C>) -> Result<(), io::Error> {
        todo!()
    }

    /// Append log entries and call the `callback` once logs are persisted on disk.
    ///
    /// It should return immediately after saving the input log entries in memory and calls the
    /// `callback` when the entries are persisted on disk, i.e., avoid blocking.
    ///
    /// This method is still async because preparing the IO is usually async.
    ///
    /// ### To ensure correctness:
    ///
    /// - When this method returns, the entries must be readable, i.e., a `LogReader` can read these
    ///   entries.
    ///
    /// - When the `callback` is called, the entries must be persisted on disk.
    ///
    ///   NOTE that: the `callback` can be called either before or after this method returns.
    ///
    /// - There must not be a **hole** in logs. Because Raft only examines the last log id to ensure
    ///   correctness.
    async fn append<I>(&mut self, entries: I, callback: IOFlushed<C>) -> Result<(), io::Error>
    where
        I: IntoIterator<Item = C::Entry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        self.write_buf.clear();
        self.write_pos.clear();
        self.write_pos.push(0);

        // prepare
        let mut first_index = 0;
        for entry in entries.into_iter() {
            let _ = entry.encode(&mut self.write_buf);
            self.write_pos.push(self.write_buf.len());

            if first_index == 0 {
                first_index = entry.index();
            }
        }
        let inputs: Vec<_> = self
            .write_pos
            .windows(2)
            .map(|i| &self.write_buf[i[0]..i[1]])
            .collect();

        // check if the store is ready
        let next_seq = self.store.next_seq();
        if next_seq == 0 {
            self.store.reset(first_index).map_err(seq_log_err)?;

        // check the log index
        } else if next_seq != first_index {
            return Err(new_other_err(format!(
                "expect log index {} but got {}",
                next_seq, first_index
            )));
        }

        // append
        self.store.append(&inputs).unwrap();

        // flush
        self.ioflush_tx
            .send(callback)
            .map_err(|_| io::Error::from(io::ErrorKind::BrokenPipe))?;

        Ok(())
    }

    /// Truncate logs after `last_log_id`, exclusive
    ///
    /// ### To ensure correctness:
    ///
    /// - It must not leave a **hole** in logs: It is OK if the truncation is not done in
    ///   transaction, but it must not leave a **hole** in logs. In other words, a non-transactional
    ///   truncation removes log entries from the end backward to this `last_log_id`.
    async fn truncate_after(&mut self, last_log_id: Option<LogIdOf<C>>) -> Result<(), io::Error> {
        let index = match last_log_id {
            Some(log_id) => log_id.index(),
            None => 0,
        };
        self.store.truncate(index).map_err(seq_log_err)
    }

    /// Purge logs up to `log_id`, inclusive
    ///
    /// ### To ensure correctness:
    ///
    /// - It must not leave a **hole** in logs.
    async fn purge(&mut self, log_id: LogIdOf<C>) -> Result<(), io::Error> {
        self.store.purge(log_id.index()).map_err(seq_log_err)
    }
}

impl<C> SeqLogStore<C>
where
    C: RaftTypeConfig,
    C::Entry: EntryCodec,
{
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, io::Error> {
        // store
        let store = if std::fs::exists(&path)? {
            SeqLog::open(path)
        } else {
            // Create one store with start-seq = 0.
            // We will reset the seq at `RaftLogStorage::append`.
            SeqLog::create(&path, 0)
        }
        .map_err(seq_log_err)?;

        // syncer and channel
        let (tx, rx) = sync_channel(1000);
        let syncer = store.syncer().map_err(seq_log_err)?;
        thread::spawn(|| background_ioflush(rx, syncer));

        // ok
        Ok(Self {
            store,
            ioflush_tx: tx,
            write_buf: Vec::new(),
            write_pos: Vec::new(),
            _p: PhantomData::default(),
        })
    }
}

// disk synchronization background thread routine
fn background_ioflush<C>(rx: Receiver<IOFlushed<C>>, mut syncer: SeqLogSyncer)
where
    C: RaftTypeConfig,
{
    while let Ok(mut iof) = rx.recv() {
        // consume all pendings
        loop {
            match rx.try_recv() {
                Ok(f) => iof = f,
                Err(TryRecvError::Empty) => break,
                Err(_) => return,
            }
        }

        // then sync
        let result = syncer.sync().map_err(seq_log_err);

        // call the last IoFlushed only
        iof.io_completed(result);
    }
}

// error type conversion
fn seq_log_err(from: Error) -> io::Error {
    match from {
        Error::Io(err) => err,
        _ => io::Error::new(io::ErrorKind::Other, from),
    }
}

// new Error with std::io::ErrorKind::Other
fn new_other_err<E>(err: E) -> io::Error
where
    E: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    io::Error::new(io::ErrorKind::Other, err)
}
