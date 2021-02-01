use std::collections::HashMap;

struct CacheHandle {
    file_id: u64,
    shard: Vec<u8>,
}

pub struct MemCache {
    cache: HashMap<u64, CacheHandle>,
}

impl MemCache {
    pub fn new() -> MemCache {
        println!("Constructor for file cache called");
        FileCache {
            cache: HashMap::new(),
        }
    }

    fn put(file_id: u64, data: &[u8]) -> () {}
}
