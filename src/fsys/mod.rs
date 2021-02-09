extern crate chrono;
extern crate fuse;
extern crate libc;
extern crate rand;
extern crate time;

use std::ffi::OsStr;
use std::path::Path;

use self::chrono::{DateTime, Utc};
use self::fuse::{
    FileAttr, FileType, Filesystem, ReplyAttr, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry,
    ReplyOpen, ReplyWrite, Request,
};
use self::libc::{EISDIR, ENOENT, ENOTDIR, ENOTEMPTY};
use self::libc::{O_ACCMODE, O_APPEND, O_RDONLY, O_RDWR, O_WRONLY};
use self::time::Timespec;
use std::time::{Duration, UNIX_EPOCH};

use db;
use db::PgDbMgr;

use fcache;

static TAG: &str = "fsys";

#[derive(Debug)]
pub struct PgDbFs {
    mount_pt: String,
    db_mgr: PgDbMgr,
    fcache: fcache::FCache,
}

pub trait DbFsUtils {
    fn make_file_entry(&self, ent: &db::Ent) -> FileAttr {
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

    fn setattr(
        &mut self,
        _req: &Request,
        _ino: u64,
        _mode: Option<u32>,
        _uid: Option<u32>,
        _gid: Option<u32>,
        _size: Option<u64>,
        _atime: Option<Timespec>,
        _mtime: Option<Timespec>,
        _fh: Option<u64>,
        _crtime: Option<Timespec>,
        _chgtime: Option<Timespec>,
        _bkuptime: Option<Timespec>,
        _flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        println!(
            "** {} - setattr (ino: {}, mode: {:?}, size: {:?}, at: {:?}, mt: {:?}",
            TAG, _ino, _mode, _size, _atime, _mtime
        );

        match self.db_mgr.lookup_by_ino(&self.mount_pt, _ino as i64) {
            None => panic!("Failed to lookup created dir"),
            Some(mut ent) => {
                //let cts: Timespec = time::now_utc().to_timespec();
                let cr_tm = match _atime {
                    Some(val) => val,
                    _ => Timespec::new(ent.create_ts.timestamp(), 0),
                };
                let up_tm = match _mtime {
                    Some(val) => val,
                    _ => Timespec::new(ent.update_ts.timestamp(), 0),
                };
                let sz = match _size {
                    Some(val) => val as i64,
                    _ => ent.size,
                };

                ent.size = sz;
                ent.create_ts =
                    DateTime::<Utc>::from(UNIX_EPOCH + Duration::from_secs(cr_tm.sec as u64));
                ent.update_ts =
                    DateTime::<Utc>::from(UNIX_EPOCH + Duration::from_secs(up_tm.sec as u64));

                let updt_count =
                    self.db_mgr
                        .setattr(&self.mount_pt, _ino as i64, sz as i64, cr_tm, up_tm);

                println!(
                    "** {}, setattr(DB update count: {}, for: {}, {})",
                    TAG, updt_count, self.mount_pt, _ino
                );

                let attr = self.make_file_entry(&ent);

                reply.attr(&attr.ctime, &attr);
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

    fn mkdir(
        &mut self,
        _req: &Request,
        _parent: u64,
        _name: &OsStr,
        _mode: u32,
        reply: ReplyEntry,
    ) {
        println!(
            "** {} - mkdir(parent: {}, name: {:?}",
            TAG,
            _parent,
            _name.to_str()
        );
        self.db_mgr
            .mkdir(&self.mount_pt, _parent as i64, &_name.to_str().unwrap());
        match self
            .db_mgr
            .lookup(&self.mount_pt, _parent as i64, &_name.to_str().unwrap())
        {
            None => panic!("Failed to lookup created dir"),
            Some(ent) => {
                let attr = self.make_file_entry(&ent);
                let ts: Timespec = Timespec::new(ent.create_ts.timestamp(), 0);
                reply.entry(&ts, &attr, 0);
            }
        }
    }

    fn unlink(&mut self, _req: &Request, _parent: u64, _name: &OsStr, reply: ReplyEmpty) {
        match self
            .db_mgr
            .lookup(&self.mount_pt, _parent as i64, &_name.to_str().unwrap())
        {
            None => reply.error(ENOENT),
            Some(ent) => {
                if ent.is_dir {
                    reply.error(EISDIR)
                } else {
                    self.db_mgr.delete_entity(&ent.id);
                    reply.ok();
                }
            }
        }
    }

    fn rename(
        &mut self,
        _req: &Request,
        _parent: u64,
        _name: &OsStr,
        _newparent: u64,
        _newname: &OsStr,
        reply: ReplyEmpty,
    ) {
        panic!("Not implemented");
    }

    fn rmdir(&mut self, _req: &Request, _parent: u64, _name: &OsStr, reply: ReplyEmpty) {
        match self
            .db_mgr
            .lookup(&self.mount_pt, _parent as i64, &_name.to_str().unwrap())
        {
            None => reply.error(ENOENT),
            Some(ent) => {
                if !ent.is_dir {
                    reply.error(ENOTDIR)
                } else if self.db_mgr.has_children(&ent.ino) {
                    reply.error(ENOTEMPTY)
                } else {
                    self.db_mgr.delete_entity(&ent.id);
                    reply.ok();
                }
            }
        }
    }

    fn open(&mut self, _req: &Request, _ino: u64, _flags: u32, reply: ReplyOpen) {
        println!("** {} - open(io: {}, flags: {})", TAG, _ino, _flags);
        print_flags(&"open", _flags as i32);
        match self.db_mgr.lookup_by_ino(&self.mount_pt, _ino as i64) {
            None => {
                println!("** {} - No entries found for ino: {}", TAG, _ino);
                reply.error(ENOENT);
            }
            Some(ent) => {
                let rw: bool = _flags as i32 & O_ACCMODE == O_RDWR;
                let wo: bool = _flags as i32 & O_ACCMODE == O_WRONLY;
                let ap: bool = _flags as i32 & O_APPEND == O_APPEND;

                if (rw || wo) && !ap {
                    println!(
                        "** {} - open({}) - File opened for write, clearning data if exists",
                        TAG, ent.id,
                    );
                    self.db_mgr.clear_file_data(&ent.id);
                }
                self.fcache.init(ent.id, _flags, ent.segment_len);
                reply.opened(_ino, _flags)
            }
        }
        //        reply.opened(0, flags);
    }

    fn read(
        &mut self,
        _req: &Request,
        _ino: u64,
        _fh: u64,
        _offset: i64,
        _size: u32,
        reply: ReplyData,
    ) {
        println!(
            "** {} - read(ino = {}, fh = {}, offset = {}, size: {}, uid: {})",
            TAG,
            _ino,
            _fh,
            _offset,
            _size,
            _req.unique()
        );
        match self.db_mgr.lookup_by_ino(&self.mount_pt, _ino as i64) {
            None => {
                println!("** {} - No entries found for ino: {}", TAG, _ino);
                reply.error(ENOENT);
            }
            Some(ent) => {
                let fb_opt = self.fcache.get(&ent.id);
                match fb_opt {
                    Some(fb) => match fb.read(_offset, _size as i32, &mut self.db_mgr) {
                        Some(data) => reply.data(data.as_slice()),
                        None => reply.error(ENOENT),
                    },
                    None => reply.error(ENOENT),
                }
            }
        }
    }

    fn write(
        &mut self,
        _req: &Request,
        _ino: u64,
        _fh: u64,
        _offset: i64,
        _data: &[u8],
        _flags: u32,
        reply: ReplyWrite,
    ) {
        println!(
            "** {} - write(ino: {}, offset: {}, data_len: {}, flags: {})",
            TAG,
            _ino,
            _offset,
            _data.len(),
            _flags
        );

        print_flags(&"write", _flags as i32);

        match self.db_mgr.lookup_by_ino(&self.mount_pt, _ino as i64) {
            None => {
                println!("** {} - No entries found for ino: {}", TAG, _ino);
                reply.error(ENOENT);
            }
            Some(ent) => {
                self.fcache.init(ent.id, _flags, ent.segment_len);
                let fb_opt = self.fcache.get(&ent.id);
                match fb_opt {
                    Some(fb) => {
                        fb.add(_offset, _data, &mut self.db_mgr);
                        return reply.written(_data.len() as u32);
                    }
                    None => {
                        println!("No sauce");
                    }
                }
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
            Some(ent) => match self.fcache.remove(&ent.id) {
                Some(mut fb) => {
                    let flags_t: i32 = fb.flags as i32;
                    if flags_t & O_ACCMODE == O_RDWR || flags_t & O_ACCMODE == O_WRONLY {
                        fb.save(&mut self.db_mgr);
                    }
                    reply.ok()
                }
                None => reply.ok(),
            },
        }
    }

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

pub fn print_flags(tag: &str, flags: i32) {
    let ro: bool = flags as i32 & O_ACCMODE == O_RDONLY;
    let rw: bool = flags as i32 & O_ACCMODE == O_RDWR;
    let wo: bool = flags as i32 & O_ACCMODE == O_WRONLY;
    let ap: bool = flags as i32 & O_APPEND == O_APPEND;

    println!(
        "** {} Flags: [value - ro-rw-wo-ap]:[{} - {}-{}-{}-{}]",
        tag, flags, ro, rw, wo, ap
    );
}

pub fn mount(path: String) {
    println!("** {}, Mounting pgdbfs on path: {}", TAG, path);
    let mountpt = Path::new(&path);
    //    let mut db_mgr = PgDbMgr::new(String::from("postgres://pgdbfs:pgdbfs@localhost/pgdbfs"));
    let mut db_mgr = PgDbMgr::new(String::from("localhost"));
    db_mgr.init();

    let pgdbfs = PgDbFs {
        mount_pt: path.to_string(),
        db_mgr: db_mgr,
        fcache: fcache::FCache::new(),
    };

    let result = fuse::mount(pgdbfs, &mountpt, &[]);
    match result {
        Ok(_r) => {
            println!("** {}, Mounting pgdbfs on path: {}", TAG, path);
        }
        Err(err) => {
            panic!(err);
        }
    }
}
