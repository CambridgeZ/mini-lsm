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

use std::cmp::{self};
use std::collections::BinaryHeap;

use anyhow::Result;

use crate::key::{self, KeySlice};

use super::StorageIterator;

struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.1
            .key()
            .cmp(&other.1.key())
            .then(self.0.cmp(&other.0))
            .reverse()
    }
}

/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, prefer the one with smaller index.
pub struct MergeIterator<I: StorageIterator> {
    iters: BinaryHeap<HeapWrapper<I>>,
    current: Option<HeapWrapper<I>>,
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(iters: Vec<Box<I>>) -> Self {
        // unimplemented!()
        let mut heap = BinaryHeap::new();
        // let mut seen_keys = std::collections::HashSet::new();

        for (i, mutiter) in iters.into_iter().enumerate() {
            if mutiter.is_valid() {
                heap.push(HeapWrapper(i, mutiter));
            }
        }

        let current = heap.pop();

        MergeIterator {
            iters: heap,
            current,
        }
    }
}

impl<I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>> StorageIterator
    for MergeIterator<I>
{
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        // unimplemented!()
        self.current.as_ref().map(|x| x.1.key()).unwrap_or_default()
    }

    fn value(&self) -> &[u8] {
        // unimplemented!()
        self.current
            .as_ref()
            .map(|x| x.1.value())
            .unwrap_or_default()
    }

    fn is_valid(&self) -> bool {
        // unimplemented!()
        self.current.is_some()
    }

    fn next(&mut self) -> Result<()> {
        if let Some(mut wrapper) = self.current.take() {
            let last_key = wrapper.1.key().to_key_vec(); // 保存上一个 key
            wrapper.1.next()?; // 当前迭代器向前
            if wrapper.1.is_valid() {
                // 如果当前迭代器还有数据，放回小根堆
                self.iters.push(wrapper);
            }

            loop {
                match self.iters.pop() {
                    Some(mut top) => {
                        if top.1.key().to_key_vec() == last_key {
                            top.1.next()?; // 移动这个 iterator
                            if top.1.is_valid() {
                                self.iters.push(top);
                            }
                            // 继续循环，找下一个 key
                        } else {
                            self.current = Some(top);
                            break;
                        }
                    }
                    None => {
                        // 没有数据了
                        self.current = None;
                        break;
                    }
                }
            }
        }

        Ok(())
    }
}

// 小结一下 MergeIterator 工作流程
// 	1.	初始化时，把所有有效 iterators 装进小根堆。
// 	2.	当前元素是堆顶最小的那个 iterator 的 (key, value)。
// 	3.	每次 next()：
// 	•	当前的 iterator 向前, 并且要保证和之前的关键字不相同，因为相同的要取最新的。
// 	•	如果还有数据，重新放回 heap。
// 	•	更新成新的堆顶。
