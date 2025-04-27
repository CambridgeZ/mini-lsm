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

use anyhow::Result;

use crate::{
    iterators::{StorageIterator, merge_iterator::MergeIterator},
    mem_table::MemTableIterator,
};

/// Represents the internal type for an LSM iterator. This type will be changed across the course for multiple times.
type LsmIteratorInner = MergeIterator<MemTableIterator>;

pub struct LsmIterator {
    inner: LsmIteratorInner,
}

impl LsmIterator {
    pub(crate) fn new(iter: LsmIteratorInner) -> Result<Self> {
        Ok(Self { inner: iter })
    }
}

impl StorageIterator for LsmIterator {
    type KeyType<'a> = &'a [u8];

    fn is_valid(&self) -> bool {
        // unimplemented!()
        return self.inner.is_valid();
    }

    fn key(&self) -> &[u8] {
        let key = self.inner.key();
        let slice = key.raw_ref(); // 或者写成 key.0 也行

        if slice.is_empty() {
            panic!("key is empty");
        }

        slice
    }

    fn value(&self) -> &[u8] {
        // unimplemented!()
        let value = self.inner.value();
        if value.is_empty() {
            panic!("value is empty");
        }
        value
    }

    fn next(&mut self) -> Result<()> {
        // unimplemented!()
        self.inner.next()?;
        if self.inner.is_valid() {
            Ok(())
        } else {
            Err(anyhow::anyhow!("Iterator is not valid"))
        }
    }
}

/// A wrapper around existing iterator, will prevent users from calling `next` when the iterator is
/// invalid. If an iterator is already invalid, `next` does not do anything. If `next` returns an error,
/// `is_valid` should return false, and `next` should always return an error.
pub struct FusedIterator<I: StorageIterator> {
    iter: I,
    has_errored: bool,
}

impl<I: StorageIterator> FusedIterator<I> {
    pub fn new(iter: I) -> Self {
        Self {
            iter,
            has_errored: false,
        }
    }
}

impl<I: StorageIterator> StorageIterator for FusedIterator<I> {
    type KeyType<'a>
        = I::KeyType<'a>
    where
        Self: 'a;

    fn is_valid(&self) -> bool {
        // unimplemented!()
        if self.has_errored {
            return false;
        } else if self.iter.is_valid() {
            return true;
        } else {
            return false;
        }
    }

    fn key(&self) -> Self::KeyType<'_> {
        // unimplemented!()
        if self.has_errored {
            panic!("Iterator is not valid");
        }
        self.iter.key()
    }

    fn value(&self) -> &[u8] {
        // unimplemented!()
        if self.has_errored {
            panic!("Iterator is not valid");
        }
        self.iter.value()
    }

    fn next(&mut self) -> Result<()> {
        // unimplemented!()
        if self.has_errored {
            return Err(anyhow::anyhow!("Iterator is not valid"));
        }
        match self.iter.next() {
            Ok(_) => Ok(()),
            Err(e) => {
                self.has_errored = true;
                Err(e)
            }
        }
    }
}
