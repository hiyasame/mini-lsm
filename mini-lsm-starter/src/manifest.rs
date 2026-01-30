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

use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use bytes::Buf;
use parking_lot::{Mutex, MutexGuard};
use serde::{Deserialize, Serialize};

use crate::compact::CompactionTask;

pub struct Manifest {
    file: Arc<Mutex<File>>,
}

#[derive(Serialize, Deserialize)]
pub enum ManifestRecord {
    Flush(usize),
    NewMemtable(usize),
    Compaction(CompactionTask, Vec<usize>),
}

impl Manifest {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            file: Arc::new(Mutex::new(
                OpenOptions::new()
                    .read(true)
                    .create_new(true)
                    .write(true)
                    .open(path)
                    .context("failed to create manifest")?,
            )),
        })
    }

    pub fn recover(path: impl AsRef<Path>) -> Result<(Self, Vec<ManifestRecord>)> {
        let mut file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(path.as_ref())
            .context("failed to recover manifest")?;
        let mut buf = Vec::<u8>::new();
        file.read_to_end(&mut buf)?;
        let manifest = Self {
            file: Arc::new(Mutex::new(file)),
        };

        let mut buf_ptr = buf.as_slice();
        let mut records = Vec::new();
        while buf_ptr.has_remaining() {
            let len = buf_ptr.get_u64();
            let slice = &buf_ptr[..len as usize];
            let json = serde_json::from_slice::<ManifestRecord>(slice)?;
            buf_ptr.advance(len as usize);
            records.push(json);
            let hash = buf_ptr.get_u32();
            if crc32fast::hash(slice) != hash {
                bail!("Manifest recovery: incorrect hash");
            }
        }

        Ok((manifest, records))
    }

    pub fn add_record(
        &self,
        _state_lock_observer: &MutexGuard<()>,
        record: ManifestRecord,
    ) -> Result<()> {
        self.add_record_when_init(record)
    }

    pub fn add_record_when_init(&self, record: ManifestRecord) -> Result<()> {
        let mut guard = self.file.lock();
        let buf = serde_json::to_vec(&record)?;
        guard.write_all(&(buf.len() as u64).to_be_bytes())?;
        guard.write_all(&buf)?;
        let hash = crc32fast::hash(&buf);
        guard.write_all(&hash.to_be_bytes())?;
        // 保证写入磁盘
        guard.sync_all()?;
        Ok(())
    }
}
