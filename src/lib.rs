use std::io;
use std::marker::PhantomData;
use std::path::Path;

use openraft::alias::{LogIdOf, VoteOf};
use openraft::storage::{IOFlushed, RaftLogStorage};
use openraft::{LogState, OptionalSend, OptionalSync, RaftLogReader, RaftTypeConfig};

use seqlog::{Error, SeqLog, SeqLogReader, SeqLogSyncer};

pub struct SeqLogStore<C>
where
    C: RaftTypeConfig,
    C::Entry: EntryCodec,
{
    store: SeqLog,
    write_buf: Vec<u8>,
    write_pos: Vec<usize>,
    _p: PhantomData<C>,
}

pub struct Reader<C>
where
    C: RaftTypeConfig,
    C::Entry: EntryCodec,
{
    inner: SeqLogReader,
    _p: PhantomData<C>,
}

pub trait EntryCodec: Sized + Send + OptionalSync + 'static {
    type EncodeError: std::fmt::Debug;
    type DecodeError: std::fmt::Debug + Send + Sync + 'static + std::error::Error;

    fn encode(&self, buf: &mut Vec<u8>) -> Result<(), Self::EncodeError>;

    fn decode(data: &[u8]) -> Result<Self, Self::DecodeError>;
}

fn seq_log_err(from: Error) -> std::io::Error {
    match from {
        Error::Io(err) => err,
        Error::EntryTooLarge(_) | Error::SeqPurged(_, _) | Error::SeqNotReached(_, _) => {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, from)
        }
        _ => std::io::Error::new(std::io::ErrorKind::InvalidData, from),
    }
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
    ) -> Result<Vec<<C as RaftTypeConfig>::Entry>, std::io::Error> {
        let start = match range.start_bound() {
            std::ops::Bound::Included(index) => *index,
            std::ops::Bound::Excluded(index) => *index + 1,
            std::ops::Bound::Unbounded => {
                return Err(std::io::Error::from(std::io::ErrorKind::InvalidInput));
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
            let entry = C::Entry::decode(buf)
                .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))?;
            res.push(entry);
        }
        Ok(res)
    }

    async fn read_vote(
        &mut self,
    ) -> Result<Option<openraft::type_config::alias::VoteOf<C>>, std::io::Error> {
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

        for entry in entries.into_iter() {
            let _ = entry.encode(&mut self.write_buf);
            self.write_pos.push(self.write_buf.len());
        }

        let inputs: Vec<_> = self
            .write_pos
            .windows(2)
            .map(|i| &self.write_buf[i[0]..i[1]])
            .collect();

        self.store.append(&inputs).unwrap();

        self.store.sync().unwrap();
        callback.io_completed(Ok(()));
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
    pub fn create<P: AsRef<Path>>(path: P) -> Result<Self, io::Error> {
        SeqLog::create(path, start_seq).map_err(seq_log_err)?;
        Ok(())
    }
}
