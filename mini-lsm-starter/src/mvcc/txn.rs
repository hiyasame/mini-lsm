// Copyright (c) 2022-2025 Alex Chi Z
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::{
    collections::HashSet,
    ops::Bound,
    sync::{Arc, atomic::AtomicBool},
};

use anyhow::Result;
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use crossbeam_skiplist::map::Entry;
use ouroboros::self_referencing;
use parking_lot::Mutex;

use crate::mem_table::map_bound;
use crate::{
    iterators::{StorageIterator, two_merge_iterator::TwoMergeIterator},
    lsm_iterator::{FusedIterator, LsmIterator},
    lsm_storage::LsmStorageInner,
};

pub struct Transaction {
    pub(crate) read_ts: u64,
    pub(crate) inner: Arc<LsmStorageInner>,
    pub(crate) local_storage: Arc<SkipMap<Bytes, Bytes>>,
    pub(crate) committed: Arc<AtomicBool>,
    /// Write set and read set
    pub(crate) key_hashes: Option<Mutex<(HashSet<u32>, HashSet<u32>)>>,
}

impl Transaction {
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        use std::sync::atomic::Ordering;
        if self.committed.load(Ordering::SeqCst) {
            panic!("cannot operate on committed txn!");
        }
        if let Some(guard) = &self.key_hashes {
            let mut guard = guard.lock();
            let (_, read_set) = &mut *guard;
            read_set.insert(farmhash::hash32(key));
        }
        if let Some(entry) = self.local_storage.get(key) {
            if entry.value().is_empty() {
                return Ok(None);
            } else {
                return Ok(Some(entry.value().clone()));
            }
        }
        self.inner.get_with_ts(key, self.read_ts)
    }

    pub fn scan(self: &Arc<Self>, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<TxnIterator> {
        use std::sync::atomic::Ordering;
        if self.committed.load(Ordering::SeqCst) {
            panic!("cannot operate on committed txn!");
        }
        let mut local_iter = TxnLocalIteratorBuilder {
            map: self.local_storage.clone(),
            iter_builder: |map| map.range((map_bound(lower), map_bound(upper))),
            item: (Bytes::new(), Bytes::new()),
        }
        .build();
        let entry = local_iter.with_iter_mut(|iter| TxnLocalIterator::entry_to_item(iter.next()));
        local_iter.with_mut(|x| *x.item = entry);

        TxnIterator::create(
            self.clone(),
            TwoMergeIterator::create(
                local_iter,
                self.inner.scan_with_ts(lower, upper, self.read_ts)?,
            )?,
        )
    }

    pub fn put(&self, key: &[u8], value: &[u8]) {
        unimplemented!()
    }

    pub fn delete(&self, key: &[u8]) {
        unimplemented!()
    }

    pub fn commit(&self) -> Result<()> {
        unimplemented!()
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {}
}

type SkipMapRangeIter<'a> =
    crossbeam_skiplist::map::Range<'a, Bytes, (Bound<Bytes>, Bound<Bytes>), Bytes, Bytes>;

#[self_referencing]
pub struct TxnLocalIterator {
    /// Stores a reference to the skipmap.
    map: Arc<SkipMap<Bytes, Bytes>>,
    /// Stores a skipmap iterator that refers to the lifetime of `TxnLocalIterator` itself.
    #[borrows(map)]
    #[not_covariant]
    iter: SkipMapRangeIter<'this>,
    /// Stores the current key-value pair.
    item: (Bytes, Bytes),
}

impl TxnLocalIterator {
    fn entry_to_item(entry: Option<Entry<'_, Bytes, Bytes>>) -> (Bytes, Bytes) {
        entry
            .map(|x| (x.key().clone(), x.value().clone()))
            .unwrap_or_else(|| (Bytes::new(), Bytes::new()))
    }
}

impl StorageIterator for TxnLocalIterator {
    type KeyType<'a> = &'a [u8];

    fn value(&self) -> &[u8] {
        &self.borrow_item().1[..]
    }

    fn key(&self) -> &[u8] {
        &self.borrow_item().0[..]
    }

    fn is_valid(&self) -> bool {
        !self.borrow_item().0.is_empty()
    }

    fn next(&mut self) -> Result<()> {
        let entry = self.with_iter_mut(|iter| TxnLocalIterator::entry_to_item(iter.next()));
        self.with_mut(|x| *x.item = entry);
        Ok(())
    }
}

pub struct TxnIterator {
    txn: Arc<Transaction>,
    iter: TwoMergeIterator<TxnLocalIterator, FusedIterator<LsmIterator>>,
}

impl TxnIterator {
    pub fn create(
        txn: Arc<Transaction>,
        iter: TwoMergeIterator<TxnLocalIterator, FusedIterator<LsmIterator>>,
    ) -> Result<Self> {
        let mut iter = Self { txn, iter };
        iter.skip_deletes()?;
        if iter.is_valid() {
            iter.add_to_read_set(iter.key());
        }
        Ok(iter)
    }

    fn skip_deletes(&mut self) -> Result<()> {
        while self.iter.is_valid() && self.iter.value().is_empty() {
            self.iter.next()?;
        }
        Ok(())
    }
    fn add_to_read_set(&self, key: &[u8]) {
        if let Some(guard) = &self.txn.key_hashes {
            let mut guard = guard.lock();
            let (_, read_set) = &mut *guard;
            read_set.insert(farmhash::hash32(key));
        }
    }
}

impl StorageIterator for TxnIterator {
    type KeyType<'a>
        = &'a [u8]
    where
        Self: 'a;

    fn value(&self) -> &[u8] {
        self.iter.value()
    }

    fn key(&self) -> Self::KeyType<'_> {
        self.iter.key()
    }

    fn is_valid(&self) -> bool {
        self.iter.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        self.iter.next()?;
        self.skip_deletes()?;
        if self.is_valid() {
            self.add_to_read_set(self.key());
        }
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.iter.num_active_iterators()
    }
}
