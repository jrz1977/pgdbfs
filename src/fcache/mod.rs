use db::PgDbMgr;
use std::cmp;
use std::collections::HashMap;
use std::fmt;
use std::iter::FromIterator;

static TAG: &str = "FCache";

#[derive(Debug)]
pub struct FSegment {
    pub file_id: i64,
    pub segment_no: i64,
    pub data: Vec<u8>,
}

impl fmt::Display for FSegment {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "File: {}, segment_num: {}, len: {}",
            self.file_id,
            self.segment_no,
            self.data.len()
        )
    }
}

impl FSegment {
    pub fn new(id: i64, sno: i64, len: i32) -> FSegment {
        FSegment {
            file_id: id,
            segment_no: sno,
            data: Vec::with_capacity(len as usize),
        }
    }

    pub fn len(&self) -> usize {
        return self.data.len();
    }
}

#[derive(Debug)]
pub struct FBuffer {
    pub file_id: i64,
    pub segment_len: i32,
    pub segments: Vec<FSegment>,
    pub flags: u32,
}

impl fmt::Display for FBuffer {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "File: {}, num_segments: {}",
            self.file_id,
            self.segments.len()
        )
    }
}

impl FBuffer {
    pub fn new(id: i64, slen: i32, flags: u32) -> FBuffer {
        FBuffer {
            file_id: id,
            segment_len: slen,
            segments: Vec::new(),
            flags: flags,
        }
    }

    pub fn add(&mut self, offset: i64, data: &[u8], db: &mut PgDbMgr) -> i32 {
        let segment_no_for_offset = self.get_segment_no(offset);
        debug!(
            "** {} add(segment_no_for_offset: {} = {})",
            TAG, offset, segment_no_for_offset
        );
        if db.check_segment_exists(&self.file_id, &segment_no_for_offset) {
            self.get_or_load_segment(&segment_no_for_offset, db);
        }

        if self.segments.is_empty() {
            let seg = FSegment::new(self.file_id, segment_no_for_offset, self.segment_len);
            self.segments.push(seg);
        } else if self.segments.last().unwrap().len() == self.segment_len as usize {
            let seg_no = self.segments.last().unwrap().segment_no + 1;
            let seg = FSegment::new(self.file_id, seg_no, self.segment_len);
            self.segments.push(seg);
        }

        let last_seg_space = cmp::min(
            self.segment_len as usize - self.segments.last().unwrap().len(),
            data.len(),
        );

        self.segments
            .last_mut()
            .unwrap()
            .data
            .extend(data[0..last_seg_space].iter().copied());

        let rem = &data[last_seg_space..];
        let iter = rem.chunks(self.segment_len as usize);
        for chunk in iter {
            let seg_no = self.segments.last().unwrap().segment_no + 1;
            let mut new_seg = FSegment::new(self.file_id, seg_no, self.segment_len);
            new_seg.data.extend(chunk.to_vec());
            self.segments.push(new_seg);
        }
        self.trim_segments(db);

        return 0;
    }

    pub fn trim_segments(&mut self, db: &mut PgDbMgr) {
        if self.segments.len() > 3 {
            let end = self.segments.len() - 2;
            let tsegments: Vec<_> = self.segments.drain(0..end).collect();
            for s in tsegments {
                db.writep(&self.file_id, &s.segment_no, &s.data);
            }
        }
    }

    pub fn save(&mut self, db: &mut PgDbMgr) -> i64 {
        debug!("Save called: {}", self.file_id);
        let mut total_written: i64 = 0;
        for s in self.segments.iter() {
            db.writep(&self.file_id, &s.segment_no, &s.data);
            total_written += s.len() as i64;
        }
        return total_written;
    }

    pub fn read(&mut self, offset: i64, size: i32, db: &mut PgDbMgr) -> Option<Vec<u8>> {
        debug!(
            "** {} read(id: {} offset = {}, len: {}",
            TAG, self.file_id, offset, size
        );
        let segment_nos = self.get_segment_nos(offset, size);
        match segment_nos {
            Some(segs) => {
                let mut read_data: Vec<u8> = Vec::new();
                let mut offset_t: i64 = offset;
                let mut size_t: i32 = size;
                for i in 0..segs.len() {
                    let seg_num: i64 = segs[i] as i64;

                    let segment_idx = self.get_or_load_segment(&seg_num, db);
                    //println!("{}, {}, {}", offset, size, segment_idx);
                    let segment = &self.segments[segment_idx as usize];
                    let offset_in_seg: i64 = offset_t - (seg_num * self.segment_len as i64);

                    let size_in_seg: i32 =
                        cmp::min((segment.len() as i64 - offset_in_seg) as i32, size_t);
                    debug!(
                        "** {} read(file_id: {}, seg_idx: {}, offset_in_seg: {}, size_in_seg: {}",
                        TAG, self.file_id, segs[i], offset_in_seg, size_in_seg
                    );

                    let t1: usize = offset_in_seg as usize;
                    let t2: usize = (offset_in_seg + size_in_seg as i64) as usize;
                    let part = Vec::from_iter(segment.data[t1..t2].iter().cloned());

                    read_data.extend(part);
                    offset_t += size_in_seg as i64;
                    size_t -= size_in_seg;
                }
                return Some(read_data);
            }
            None => None,
        }
    }

    pub fn get_segment_nos(&mut self, offset: i64, size: i32) -> Option<Vec<i32>> {
        let offset_end: i64 = offset + size as i64;
        let first_seg_no = self.get_segment_no(offset);
        let last_seg_no = self.get_segment_no(offset_end);
        debug!(
            "** {} - get_segment_indexes(st: {}, en: {}, range: {} - {})",
            TAG, offset, offset_end, first_seg_no, last_seg_no,
        );

        let mut segment_nos: Vec<i32> = Vec::new();
        for i in first_seg_no..last_seg_no + 1 {
            segment_nos.push(i as i32);
        }
        return Some(segment_nos);
    }

    fn get_segment_no(&mut self, offset: i64) -> i64 {
        return offset / self.segment_len as i64;
    }

    fn get_or_load_segment(&mut self, segment_no: &i64, db: &mut PgDbMgr) -> i64 {
        // Check if exists in local cache
        let existing_idx = self.get_segment_cache(segment_no);
        debug!("Existing idx: {}", existing_idx);
        if existing_idx == -1 {
            match db.load_segment(&self.file_id, segment_no) {
                Some(bytes) => {
                    let s = FSegment {
                        file_id: self.file_id,
                        segment_no: *segment_no,
                        data: bytes,
                    };
                    self.segments.push(s);
                }
                None => {
                    debug!("Row not found in db");
                    error!("No segment in db for segment_no: {}", segment_no);

                    let potential_offset = segment_no * self.segment_len as i64;

                    let file_sz = db.get_file_sz(&self.file_id);

                    if potential_offset >= file_sz {
                        debug!("offset exceeds file size, returning last segment");
                        return self.segments.len() as i64 - 1;
                    }
                    panic!(
                        "Invalid offset for file: {}, segment_no: {}",
                        self.file_id, segment_no
                    );
                }
            }

            return self.get_segment_cache(segment_no);
        }
        existing_idx
    }

    fn get_segment_cache(&mut self, segment_no: &i64) -> i64 {
        for (i, s) in self.segments.iter().enumerate() {
            if s.segment_no == *segment_no {
                return i as i64;
            }
        }
        debug!("Get segment cache not found {}", self.segments.len());

        return -1;
    }
}

#[derive(Debug)]
pub struct FCache {
    pub fcache: HashMap<String, FBuffer>,
}

impl fmt::Display for FCache {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Cache size: {}", self.fcache.len())
    }
}

impl FCache {
    pub fn new() -> FCache {
        FCache {
            fcache: HashMap::new(),
        }
    }

    pub fn get(&mut self, mnt_pt: &String, ino: &i64) -> Option<&mut FBuffer> {
        let key = self.make_key(mnt_pt, ino);
        self.fcache.get_mut(&key)
    }

    pub fn get_cached(
        &mut self,
        mnt_pt: &String,
        ino: i64,
        flags: u32,
        db: &mut PgDbMgr,
    ) -> Option<&mut FBuffer> {
        let key = self.make_key(mnt_pt, &ino);
        if self.fcache.contains_key(&key) {
            return self.fcache.get_mut(&key);
        }
        debug!("Cached file not found, looking up in db {}", ino);
        match db.lookup_by_ino(mnt_pt, ino as i64) {
            None => {
                debug!("No entries found for ino: {}", ino);
                None
            }
            Some(ent) => {
                self.init(mnt_pt, ino, ent.id, flags, ent.segment_len);
                self.fcache.get_mut(&key)
            }
        }
    }

    pub fn remove(&mut self, mnt_pt: &String, ino: &i64) -> Option<FBuffer> {
        let key = self.make_key(mnt_pt, ino);
        self.fcache.remove(&key)
    }

    pub fn init(&mut self, mnt_pt: &String, ino: i64, id: i64, flags: u32, segment_len: i32) {
        debug!("Caching file: mnt_pt: {}, ino: {})", mnt_pt, ino);
        let key = self.make_key(mnt_pt, &ino);
        //info!("Key = {}", key);
        self.fcache
            .entry(key)
            .or_insert_with(|| FBuffer::new(id, segment_len, flags));
    }

    fn make_key(&mut self, mnt_pt: &String, ino: &i64) -> String {
        //let mut key: String = &[mnt_pt, &ino.to_string()].join();
        let mut key: String = String::new();
        key.push_str(mnt_pt);
        key.push_str("-");
        key.push_str(&ino.to_string());
        return key;
    }
}
