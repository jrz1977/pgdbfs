extern crate fuse;
extern crate libc;
extern crate rand;
extern crate time;

use std::ffi::OsStr;
use std::path::Path;

use self::fuse::{
    FileAttr, FileType, Filesystem, ReplyAttr, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry,
    ReplyOpen, ReplyWrite, Request,
};
use self::libc::{ENOENT, ENOSYS};
use self::time::Timespec;

use db;
use db::PgDbMgr;

mod memcache;

static TAG: &str = "fsys";

#[derive(Debug)]
pub struct PgDbFs {
    mount_pt: String,
    db_mgr: PgDbMgr,
    cache_mgr: memcache::MemCache,
    read_cache_mgr: memcache::MemCache,
}

pub trait DbFsUtils {
    fn make_file_entry(&self, ent: &db::Ent) -> FileAttr {
        let ttl = Timespec::new(1, 0);
        let attr = FileAttr {
            ino: ent.ino as u64,
            size: ent.size as u64,
            blocks: 0,
            //            atime: ent.create_ts,
            atime: Timespec::new(ent.create_ts.timestamp(), 0),
            mtime: Timespec::new(ent.update_ts.timestamp(), 0),
            ctime: Timespec::new(ent.update_ts.timestamp(), 0),
            crtime: Timespec::new(ent.create_ts.timestamp(), 0),
            kind: if ent.is_dir {
                FileType::Directory
            } else {
                FileType::RegularFile
            },
            perm: 0o644,
            nlink: 0,
            uid: 0,
            gid: 0,
            rdev: 0,
            flags: 0,
        };
        return attr;
    }
}

impl DbFsUtils for PgDbFs {}

impl Filesystem for PgDbFs {
    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        println!(
            "** {} - lookup(parent={}, name={:?})",
            TAG,
            parent,
            name.to_str()
        );
        match name.to_str() {
            None => {
                println!("No value in name, parent: {}", parent);
                return;
            }
            Some(n) => {
                match self.db_mgr.lookup(&self.mount_pt, parent as i64, n) {
                    None => {
                        println!("No entries found for parent: {}, name: {:?}", parent, name);
                        reply.error(ENOENT);
                    }
                    Some(ent) => {
                        //                        let ts = time::now().to_timespec();
                        let ttl = Timespec::new(1, 0);
                        let attr = self.make_file_entry(&ent);
                        reply.entry(&ttl, &attr, 0);
                    }
                }
            }
        }
    }

    fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
        println!("** {} - getattr(ino={})", TAG, ino);

        match self.db_mgr.lookup_by_ino(&self.mount_pt, ino as i64) {
            None => println!("** {} - No entries found for ino: {}", TAG, ino),
            Some(ent) => {
                let attr = self.make_file_entry(&ent);
                let ts: Timespec = Timespec::new(ent.create_ts.timestamp(), 0);
                //                reply.attr(&ent.create_ts, &attr);
                reply.attr(&ts, &attr);
            }
        }
    }
    //

    fn setattr(
        &mut self,
        _req: &Request,
        ino: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<Timespec>,
        mtime: Option<Timespec>,
        fh: Option<u64>,
        crtime: Option<Timespec>,
        chgtime: Option<Timespec>,
        bkuptime: Option<Timespec>,
        flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        println!(
            "** {} - setattr (ino: {}, mode: {:?}, size: {:?}, at: {:?}, mt: {:?}",
            TAG, ino, mode, size, atime, mtime
        );
        let cts: Timespec = time::now_utc().to_timespec();
        let cr_tm = match atime {
            Some(val) => val,
            _ => cts,
        };
        let up_tm = match mtime {
            Some(val) => val,
            _ => cts,
        };
        let sz = match size {
            Some(val) => val,
            _ => 0,
        };
        let updt_count = self
            .db_mgr
            .setattr(&self.mount_pt, ino as i64, sz as i64, cr_tm, up_tm);

        println!(
            "** {}, setattr(DB update count: {}, for: {}, {}",
            TAG, updt_count, self.mount_pt, ino
        );

        match self.db_mgr.lookup_by_ino(&self.mount_pt, ino as i64) {
            None => panic!("Failed to lookup created dir"),
            Some(ent) => {
                let attr = self.make_file_entry(&ent);
                let ts: Timespec = Timespec::new(ent.create_ts.timestamp(), 0);
                reply.attr(&ts, &attr);
            }
        }
    }

    fn mknod(
        &mut self,
        _req: &Request,
        parent: u64,
        name: &OsStr,
        mode: u32,
        rdev: u32,
        reply: ReplyEntry,
    ) {
        println!(
            "** {} - mknod (parent: {}, mode: {}, rdev: {}, name: {:?}",
            TAG,
            parent,
            mode,
            rdev,
            name.to_str()
        );

        self.db_mgr
            .mkfile(&self.mount_pt, parent as i64, &name.to_str().unwrap());
        match self
            .db_mgr
            .lookup(&self.mount_pt, parent as i64, &name.to_str().unwrap())
        {
            None => panic!("Failed to lookup created dir"),
            Some(ent) => {
                let attr = self.make_file_entry(&ent);
                let ts: Timespec = Timespec::new(ent.create_ts.timestamp(), 0);
                reply.entry(&ts, &attr, 0);
            }
        }
    }

    fn mkdir(&mut self, _req: &Request, parent: u64, name: &OsStr, mode: u32, reply: ReplyEntry) {
        println!(
            "** {} - mkdir(parent: {}, name: {:?}",
            TAG,
            parent,
            name.to_str()
        );
        self.db_mgr
            .mkdir(&self.mount_pt, parent as i64, &name.to_str().unwrap());
        match self
            .db_mgr
            .lookup(&self.mount_pt, parent as i64, &name.to_str().unwrap())
        {
            None => panic!("Failed to lookup created dir"),
            Some(ent) => {
                let attr = self.make_file_entry(&ent);
                let ts: Timespec = Timespec::new(ent.create_ts.timestamp(), 0);
                reply.entry(&ts, &attr, 0);
            }
        }
    }

    fn open(&mut self, _req: &Request, ino: u64, flags: u32, reply: ReplyOpen) {
        println!("** {} - open(ino: {}, flags: {})", TAG, ino, flags);

        match self.db_mgr.lookup_by_ino(&self.mount_pt, ino as i64) {
            None => {
                println!("** {} - No entries found for ino: {}", TAG, ino);
                reply.error(ENOENT);
            }
            Some(ent) => reply.opened(ino, flags),
        }
        //        reply.opened(0, flags);
    }

    fn read(&mut self, req: &Request, ino: u64, fh: u64, offset: i64, size: u32, reply: ReplyData) {
        println!(
            "** {} - read(ino = {}, fh = {}, offset = {}, size: {}, uid: {})",
            TAG,
            ino,
            fh,
            offset,
            size,
            req.unique()
        );
        match self.db_mgr.lookup_by_ino(&self.mount_pt, ino as i64) {
            None => {
                println!("** {} - No entries found for ino: {}", TAG, ino);
                reply.error(ENOENT);
            }
            Some(ent) => match self.db_mgr.read(ent.id, offset, size) {
                None => {
                    return reply.error(ENOENT);
                }
                Some(data) => {
                    return reply.data(data.as_slice());
                }
            },
        }
    }

    // fn read(&mut self, req: &Request, ino: u64, fh: u64, offset: i64, size: u32, reply: ReplyData) {
    //     println!(
    //         "** {} - read(ino = {}, fh = {}, offset = {}, size: {}, uid: {})",
    //         TAG,
    //         ino,
    //         fh,
    //         offset,
    //         size,
    //         req.unique()
    //     );
    //     match self.db_mgr.lookup_by_ino(&self.mount_pt, ino as i64) {
    //         None => {
    //             println!("** {} - No entries found for ino: {}", TAG, ino);
    //             reply.error(ENOENT);
    //         }
    //         Some(ent) => match self.db_mgr.read(ent.id, offset, size) {
    //             None => {
    //                 return reply.error(ENOENT);
    //             }
    //             Some(data) => {
    //                 return reply.data(data.as_slice());
    //             }
    //         },
    //     }
    // }

    fn write(
        &mut self,
        _req: &Request,
        ino: u64,
        fh: u64,
        offset: i64,
        data: &[u8],
        flags: u32,
        reply: ReplyWrite,
    ) {
        println!(
            "** {} - write(ino: {}, offset: {}, data_len: {}, flags: {})",
            TAG,
            ino,
            offset,
            data.len(),
            flags
        );

        match self.db_mgr.lookup_by_ino(&self.mount_pt, ino as i64) {
            None => {
                println!("** {} - No entries found for ino: {}", TAG, ino);
                reply.error(ENOENT);
            }
            Some(ent) => {
                let ws: memcache::MemCachePutReply = self.cache_mgr.put(&ent.id, data);
                println!(
                    "** {} WriteStatus - {}, offset: {}, len: {}",
                    TAG,
                    ws,
                    offset,
                    data.len(),
                );
                match ws.write_status {
                    memcache::WriteStatus::Unknown => reply.error(ENOENT),
                    memcache::WriteStatus::Buffered => reply.written(data.len() as u32),
                    memcache::WriteStatus::BufferFilled => {
                        let cached_data = &ws.data.unwrap();
                        let offset_st = ws.offset_en - cached_data.len();
                        self.db_mgr.write(
                            &self.mount_pt,
                            ino as i64,
                            offset_st as i64,
                            ws.offset_en as i64,
                            cached_data,
                        );
                        return reply.written(data.len() as u32);
                    }
                }
                //return reply.written(data.len() as u32);
            }
        }
    }

    fn flush(&mut self, _req: &Request, _ino: u64, _fh: u64, _lock_owner: u64, reply: ReplyEmpty) {
        println!(
            "** {} - flush(ino: {} fh: {}, uid: {})",
            TAG,
            _ino,
            _fh,
            _req.unique()
        );

        match self.db_mgr.lookup_by_ino(&self.mount_pt, _ino as i64) {
            None => {
                println!("** {} - No entries found for ino: {}", TAG, _ino);
                reply.error(ENOENT);
            }
            Some(ent) => match self.cache_mgr.remove(&ent.id) {
                Some(c) => {
                    let cached_data = &c.data.unwrap();
                    let offset_st = c.offset_en - cached_data.len();
                    println!(
                        "Flushing file: {}, off: {} - {}",
                        _ino, offset_st, c.offset_en
                    );
                    self.db_mgr.write(
                        &self.mount_pt,
                        _ino as i64,
                        offset_st as i64,
                        c.offset_en as i64,
                        cached_data,
                    );
                    reply.ok()
                }
                None => reply.ok(),
            },
        }
    }

    // fn fsync(&mut self, _req: &Request, _ino: u64, _fh: u64, _datasync: bool, reply: ReplyEmpty) {
    //     println!("** {} - fsync(ino: {})", TAG, _ino);

    //     match self.db_mgr.lookup_by_ino(&self.mount_pt, _ino as i64) {
    //         None => {
    //             println!("** {} - No entries found for ino: {}", TAG, _ino);
    //             reply.error(ENOSYS);
    //         }
    //         Some(ent) => match self.cache_mgr.get(&ent.id) {
    //             Some(c) => {
    //                 let cached_data = &c.data.unwrap();
    //                 let offset_st = c.offset_en - cached_data.len();
    //                 println!(
    //                     "Flushing file: {}, off: {} - {}",
    //                     _ino, offset_st, c.offset_en
    //                 );
    //                 self.db_mgr.write(
    //                     &self.mount_pt,
    //                     _ino as i64,
    //                     offset_st as i64,
    //                     c.offset_en as i64,
    //                     cached_data,
    //                 );
    //                 reply.ok()
    //             }
    //             None => reply.error(ENOSYS),
    //         },
    //     }
    // }

    //
    fn readdir(
        &mut self,
        _req: &Request,
        ino: u64,
        fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        println!(
            "** {} - readdir(ino={}, fh={}, offset={}, mnt_pt: {})",
            TAG, ino, fh, offset, self.mount_pt
        );
        let off = 0;
        if offset == 0 {
            let entries: Vec<db::Ent> = self.db_mgr.ls(self.mount_pt.to_string(), ino as i64);
            reply.add(ino, off + 1, FileType::Directory, &Path::new("."));
            reply.add(ino, off + 1, FileType::Directory, &Path::new(".."));

            for e in entries {
                //println!("** {} - Adding ent: {}", TAG, e.name);
                reply.add(
                    e.ino as u64,
                    off + 1,
                    if e.is_dir {
                        FileType::Directory
                    } else {
                        FileType::RegularFile
                    },
                    &Path::new(&e.name),
                );
            }
        }
        reply.ok();
    }
}

pub fn mount(path: String) {
    println!("** {}, Mounting pgdbfs on path: {}", TAG, path);
    let mountpt = Path::new(&path);
    //    let mut db_mgr = PgDbMgr::new(String::from("postgres://pgdbfs:pgdbfs@localhost/pgdbfs"));
    let mut db_mgr = PgDbMgr::new(String::from("localhost"));
    db_mgr.init();

    let mut memcache = memcache::MemCache::new();

    let pgdbfs = PgDbFs {
        mount_pt: path.to_string(),
        db_mgr: db_mgr,
        cache_mgr: memcache::MemCache::new(),
        read_cache_mgr: memcache::MemCache::new(),
    };

    let d: [u8; 0] = [];

    // memcache.put(9, &d);
    // memcache.put(19, &d);

    fuse::mount(pgdbfs, &mountpt, &[]);
    println!("** {}, Mounting pgdbfs on path: {}", TAG, path);
}
