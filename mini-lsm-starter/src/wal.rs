// REMOVE THIS LINE after fully implementing this functionality
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

use crate::key::{KeyBytes, KeySlice};
use anyhow::{Context, Result, bail};
use bytes::{Buf, BufMut, Bytes};
use crossbeam_skiplist::SkipMap;
use nom::AsBytes;
use parking_lot::Mutex;
use std::fs::{File, OpenOptions};
use std::hash::Hasher;
use std::io::{BufWriter, Read, Write};
use std::path::Path;
use std::sync::Arc;

pub struct Wal {
    file: Arc<Mutex<BufWriter<File>>>,
}

impl Wal {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(File::create(path)?))),
        })
    }

    pub fn recover(path: impl AsRef<Path>, skiplist: &SkipMap<KeyBytes, Bytes>) -> Result<Self> {
        let mut file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(path)
            .context("failed to recover from WAL")?;
        let mut buf: Vec<u8> = Vec::new();
        file.read_to_end(&mut buf)?;
        let mut buf = buf.as_slice();
        while buf.has_remaining() {
            let batch_size = buf.get_u32() as usize;
            let mut batch_buf = &buf[..batch_size];
            let mut kv_pairs = Vec::new();
            let mut hasher = crc32fast::Hasher::new();
            while batch_buf.has_remaining() {
                let key_len = batch_buf.get_u16() as usize;
                hasher.write(&(key_len as u16).to_be_bytes());
                let key = Bytes::copy_from_slice(&batch_buf[..key_len]);
                hasher.write(key.as_bytes());
                batch_buf.advance(key_len);
                let ts = batch_buf.get_u64();
                hasher.write(&ts.to_be_bytes());
                let value_len = batch_buf.get_u16() as usize;
                hasher.write(&(value_len as u16).to_be_bytes());
                let value = Bytes::copy_from_slice(&batch_buf[..value_len]);
                hasher.write(value.as_bytes());
                batch_buf.advance(value_len);
                kv_pairs.push((key, ts, value));
            }
            buf.advance(batch_size);
            let actual_hash = buf.get_u32();
            if hasher.finish() as u32 != actual_hash {
                bail!("WAL recovery: incorrect hash");
            }
            for (key, ts, value) in kv_pairs {
                skiplist.insert(KeyBytes::from_bytes_with_ts(key, ts), value);
            }
        }

        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    pub fn put(&self, key: KeySlice, value: &[u8]) -> Result<()> {
        self.put_batch(&[(key, value)])
    }

    /// Implement this in week 3, day 5; if you want to implement this earlier, use `&[u8]` as the key type.
    pub fn put_batch(&self, data: &[(KeySlice, &[u8])]) -> Result<()> {
        let mut file = self.file.lock();
        let mut buf = Vec::<u8>::new();
        for (key, value) in data {
            buf.put_u16(key.key_len() as u16);
            buf.put_slice(key.key_ref());
            buf.put_u64(key.ts());
            buf.put_u16(value.len() as u16);
            buf.put_slice(value);
        }
        // write batch_size header (u32)
        file.write_all(&(buf.len() as u32).to_be_bytes())?;
        // write key-value pairs body
        file.write_all(&buf)?;
        // write checksum (u32)
        file.write_all(&crc32fast::hash(&buf).to_be_bytes())?;
        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        let mut guard = self.file.lock();
        guard.flush()?;
        guard.get_mut().sync_all()?;
        Ok(())
    }
}
