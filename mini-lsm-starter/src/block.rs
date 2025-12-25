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

mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::{Buf, BufMut, Bytes};
pub use iterator::BlockIterator;

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    pub(crate) fn get_first_key(&self) -> crate::key::KeyVec {
        if self.data.is_empty() {
            return crate::key::KeyVec::new();
        }
        let mut buf = &self.data[..];
        buf.get_u16(); // skip overlap_len (should be 0 for first key)
        let key_len = buf.get_u16();
        let key = &buf[..key_len as usize];
        crate::key::KeyVec::from_vec(key.to_vec())
    }

    /// Encode the internal data to the data layout illustrated in the course
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        // data section             offset section              extra
        // entry1 entry2 ... entryN offset1 offset2 ... offsetN num_of_elements
        // entry
        // key_len(2 byte) key (key_len) value_len(2 byte) value (value_len)
        let mut bytes: Vec<u8> = self.data.clone();
        for x in &self.offsets {
            bytes.put_u16(*x);
        }
        bytes.put_u16(self.offsets.len() as u16);
        Bytes::from(bytes)
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        // 先读 num of elements
        let num_of_elements = (&data[data.len() - 2..]).get_u16() as usize;
        let offsets: Vec<u16> = (&data[data.len() - 2 - num_of_elements * 2..data.len() - 2])
            .chunks(2)
            .map(|chunk| {
                let mut chunk = &chunk[..];
                chunk.get_u16()
            })
            .collect();
        let data_section: Vec<u8> = (&data[0..data.len() - num_of_elements * 2 - 2]).to_vec();
        Block {
            data: data_section,
            offsets,
        }
    }
}
