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

use std::ptr::dangling;

use nom::sequence::tuple;
use rustyline::completion::Pair;

use crate::key::{KeySlice, KeyVec};

use super::Block;

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,

    /// All serialized key-value pairs in the block.
    data: Vec<u8>,

    /// The expected block size.
    block_size: usize,

    /// The first key in the block
    first_key: KeyVec,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            offsets: Vec::new(),
            data: Vec::new(),
            block_size,
            first_key: KeyVec::new(),
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        // unimplemented!()
        if self.is_empty() {
            self.first_key = key.to_key_vec();
            self.offsets.push(0);
        }
        let key_len: u16 = key.len() as u16;
        let value_len = value.len() as u16;
        let entry_len = key_len + value_len + 2 * std::mem::size_of::<u16>() as u16;

        let last_offset = *self.offsets.last().unwrap_or(&0);

        if last_offset + entry_len as u16 > self.block_size  as u16 {
            return false;
        }
        else {
            if last_offset != 0 {
                self.offsets.push(last_offset + entry_len as u16);
            }
            self.data.push((key_len>>8) as u8);
            self.data.push((key_len&0xFF) as u8);
            let key_vec = key.to_key_vec();
            // 将key_vec 里面的u8全部push进data
            self.data.append(key_vec);


            return true;
        }

        

        
        
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        // unimplemented!()
        return self.offsets.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        unimplemented!()
    }
}
