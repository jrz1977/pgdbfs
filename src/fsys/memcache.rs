use std::collections::HashMap;

use std::fmt;

const MAX_BUFFER_SZ: usize = 131072 * 2;
//const MAX_BUFFER_SZ: usize = 1048576;
//const MAX_BUFFER_SZ: usize = 16384000;
//const MAX_BUFFER_SZ: usize = 4194304;

pub enum WriteStatus {
    Unknown,
    Buffered,
    BufferFilled,
}

pub struct MemCachePutReply {
    pub write_status: WriteStatus,
    pub data: Option<Vec<u8>>,
    pub offset_en: usize,
}

pub struct MemCacheGetReply {
    pub data: Option<Vec<u8>>,
    pub offset_en: usize,
}

impl MemCachePutReply {
    pub fn new() -> MemCachePutReply {
        MemCachePutReply {
            write_status: WriteStatus::Unknown,
            data: None,
            offset_en: 0,
        }
    }
}

impl MemCacheGetReply {
    pub fn new() -> MemCacheGetReply {
        MemCacheGetReply {
            data: None,
            offset_en: 0,
        }
    }
}

impl fmt::Display for MemCachePutReply {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.write_status {
            WriteStatus::Unknown => write!(f, "UNKNOWN"),
            WriteStatus::Buffered => write!(f, "BUFFERED"),
            WriteStatus::BufferFilled => write!(f, "BUFFER_FILLED"),
        }
    }
}

#[derive(Debug)]
struct CacheHandle {
    file_id: i64,
    shard: Vec<u8>,
    offset_en: usize,
}

impl CacheHandle {
    pub fn new() -> CacheHandle {
        CacheHandle {
            file_id: 0,
            shard: Vec::new(),
            offset_en: 0,
        }
    }
}

#[derive(Debug)]
pub struct MemCache {
    cache: HashMap<i64, CacheHandle>,
}

impl MemCache {
    pub fn new() -> MemCache {
        MemCache {
            cache: HashMap::new(),
        }
    }

    pub fn get(&mut self, file_id: &i64) -> Option<MemCacheGetReply> {
        match self.cache.get_mut(file_id) {
            Some(c) => {
                let mut reply: MemCacheGetReply = MemCacheGetReply::new();
                reply.offset_en = c.offset_en;
                let copy: Vec<u8> = c.shard.drain(0..).collect();
                reply.data = Some(copy);
                Some(reply)
            }
            None => None,
        }
    }

    // pub fn get_by_offset(
    //     &mut self,
    //     file_id: &i64,
    //     offset: &i64,
    //     size: &i64,
    // ) -> Option<MemCacheGetReply> {
    //     match self.cache.get_mut(file_id) {
    //         Some(c) => {
    //             let mut reply: MemCacheGetReply = MemCacheGetReply::new();
    //             if (c.offset_en >= offset && c.offset_en < (offset + size)) {
    //                 reply.offset_en = c.offset_en;
    //                 let copy: Vec<u8> = c.shard.drain(0..).collect();
    //                 reply.data = Some(copy);
    //                 Some(reply)
    //             } else {
    //                 None
    //             }
    //         }
    //         None => None,
    //     }
    // }

    pub fn remove(&mut self, file_id: &i64) -> Option<MemCacheGetReply> {
        match self.cache.remove(file_id) {
            Some(mut c) => {
                let mut reply: MemCacheGetReply = MemCacheGetReply::new();
                reply.offset_en = c.offset_en;
                let copy: Vec<u8> = c.shard.drain(0..).collect();
                reply.data = Some(copy);
                Some(reply)
            }
            None => None,
        }
    }

    pub fn put(&mut self, file_id: &i64, data: &[u8]) -> MemCachePutReply {
        let mut reply: MemCachePutReply = MemCachePutReply::new();
        match self.cache.get_mut(file_id) {
            Some(c) => {
                c.shard.extend(data.iter().copied());
                c.offset_en += data.len();

                println!(
                    "Copied into cache entry for: {}, curr shard size: {}, total: {}",
                    file_id,
                    c.shard.len(),
                    c.offset_en
                );
                if c.shard.len() >= MAX_BUFFER_SZ {
                    reply.write_status = WriteStatus::BufferFilled;
                    let copy: Vec<u8> = c.shard.drain(0..MAX_BUFFER_SZ).collect();
                    reply.data = Some(copy);
                } else {
                    reply.write_status = WriteStatus::Buffered;
                }
                reply.offset_en = c.offset_en;
                reply
            }
            None => {
                let mut ch = CacheHandle::new();
                ch.offset_en += data.len();
                ch.file_id = *file_id;
                ch.shard.extend(data.iter().copied());

                reply.offset_en = ch.offset_en;

                reply.write_status = WriteStatus::Buffered;

                println!(
                    "Copied into cache entry for: {}, curr shard size: {}, total: {}",
                    file_id,
                    ch.shard.len(),
                    ch.offset_en
                );
                self.cache.insert(*file_id, ch);

                reply
            }
        }
    }
}
