extern crate chrono;
extern crate dirs;
extern crate fuse;
extern crate libc;
extern crate rand;
extern crate time;

use serde::{Deserialize, Serialize};
use std::ffi::OsStr;
use std::path::Path;

use self::chrono::{DateTime, Utc};
use self::fuse::{
    FileAttr, FileType, Filesystem, ReplyAttr, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry,
    ReplyOpen, ReplyWrite, Request,
};
use self::libc::{c_int, EISDIR, ENOENT, ENOTDIR, ENOTEMPTY};
use self::libc::{O_ACCMODE, O_APPEND, O_RDONLY, O_RDWR, O_WRONLY};
use self::time::Timespec;
use std::time::{Duration, UNIX_EPOCH};

use db;
use db::PgDbMgr;

use fcache;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PgDbFsConfig {
    pub db_host: String,
    pub db_user: String,
    pub db_pass: String,
    pub db_segment_len: i32,
}

impl ::std::default::Default for PgDbFsConfig {
    fn default() -> Self {
        Self {
            db_host: "localhost".to_string(),
            db_user: "pgdbfs".to_string(),
            db_pass: "pgdbfs".to_string(),
            db_segment_len: 1048576,
        }
    }
}

#[derive(Debug)]
pub struct PgDbFs {
    mount_pt: String,
    db_mgr: PgDbMgr,
    fcache: fcache::FCache,
    cfg: PgDbFsConfig,
}

pub trait DbFsUtils {
    fn calculate_num_blocks(&self, size: i64) -> u64 {
        if size < 0 {
            return 0;
        }
        let num = (size as f64 / 1024f64).ceil();
        let rem = num as f64 % 4f64;
        if rem == 0f64 {
            num as u64
        } else {
            (num + 4f64 - rem) as u64
        }
    }

    fn make_file_entry(&self, ent: &db::Ent, req: &Request) -> FileAttr {
        let blocks = self.calculate_num_blocks(ent.size) * 2;
        let attr = FileAttr {
            ino: ent.ino as u64,
            size: ent.size as u64,
            blocks: blocks,
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
            nlink: if ent.is_dir { 2 + ent.nlink as u32 } else { 1 },
            uid: req.uid(),
            gid: req.gid(),
            rdev: 0,
            flags: 0,
        };
        return attr;
    }
}

impl DbFsUtils for PgDbFs {}

impl Filesystem for PgDbFs {
    fn init(&mut self, _req: &Request) -> Result<(), c_int> {
        debug!("init({:?}", _req);
        Ok(())
    }

    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        debug!("lookup(parent={}, name={:?})", parent, name.to_str());
        match name.to_str() {
            None => {
                debug!("No value in name, parent: {}", parent);
                return;
            }
            Some(n) => match self.db_mgr.lookup(&self.mount_pt, parent as i64, n) {
                None => {
                    debug!("No entries found for parent: {}, name: {:?}", parent, name);
                    reply.error(ENOENT);
                }
                Some(ent) => {
                    let ttl = Timespec::new(1, 0);
                    let attr = self.make_file_entry(&ent, _req);
                    reply.entry(&ttl, &attr, 0);
                }
            },
        }
    }

    fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
        debug!("getattr(ino={})", ino);

        match self.db_mgr.lookup_by_ino(&self.mount_pt, ino as i64) {
            None => {
                debug!("No entries found for ino: {}", ino);
            }
            Some(ent) => {
                let attr = self.make_file_entry(&ent, _req);
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
        debug!(
            "setattr (ino: {}, mode: {:?}, size: {:?}, at: {:?}, mt: {:?}",
            _ino, _mode, _size, _atime, _mtime
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

                debug!(
                    "setattr(DB update count: {}, for: {}, {})",
                    updt_count, self.mount_pt, _ino
                );

                let attr = self.make_file_entry(&ent, _req);

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
        debug!(
            "mknod (parent: {}, mode: {}, rdev: {}, name: {:?}",
            parent,
            mode,
            rdev,
            name.to_str()
        );

        self.db_mgr.mkfile(
            &self.mount_pt,
            parent as i64,
            &name.to_str().unwrap(),
            &self.cfg.db_segment_len,
        );
        match self
            .db_mgr
            .lookup(&self.mount_pt, parent as i64, &name.to_str().unwrap())
        {
            None => panic!("Failed to lookup created dir"),
            Some(ent) => {
                let attr = self.make_file_entry(&ent, _req);
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
        debug!("mkdir(parent: {}, name: {:?}", _parent, _name.to_str());
        self.db_mgr
            .mkdir(&self.mount_pt, _parent as i64, &_name.to_str().unwrap());
        match self
            .db_mgr
            .lookup(&self.mount_pt, _parent as i64, &_name.to_str().unwrap())
        {
            None => panic!("Failed to lookup created dir"),
            Some(ent) => {
                let attr = self.make_file_entry(&ent, _req);
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
        let dst_dir = self.db_mgr.lookup_by_ino(&self.mount_pt, _newparent as i64);
        match dst_dir {
            None => {
                error!(
                    "Dst dir does not exist: mnt: {} ino: {}",
                    self.mount_pt, _newparent
                );
                reply.error(ENOENT);
                return;
            }
            Some(ent) => {
                if !ent.is_dir {
                    error!(
                        "Dst is not a directory, mnt: {}, ino: {}",
                        self.mount_pt, _newparent
                    );
                    reply.error(ENOTDIR);
                    return;
                }
                let src_file =
                    self.db_mgr
                        .lookup(&self.mount_pt, _parent as i64, &_name.to_str().unwrap());
                match src_file {
                    None => {
                        error!(
                            "Src file does not exist, mnt: {}, ino: {}, file: {}",
                            self.mount_pt,
                            _parent,
                            _name.to_str().unwrap()
                        );
                        reply.error(ENOENT);
                        return;
                    }
                    Some(srcent) => {
                        match self.db_mgr.lookup(
                            &self.mount_pt,
                            _newparent as i64,
                            &_newname.to_str().unwrap(),
                        ) {
                            Some(dst_file) => {
                                debug!(
                                    "Removing dst file before rename: mnt: {}, ino: {}, name: {}",
                                    &self.mount_pt, &dst_file.ino, &dst_file.name
                                );
                                self.db_mgr.delete_entity(&dst_file.id);
                            }
                            None => {}
                        }
                        self.db_mgr.update_parent(&srcent.id, &ent.ino);
                        self.db_mgr
                            .update_name(&srcent.id, _newname.to_str().unwrap());
                        reply.ok();
                    }
                }
            }
        }
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
        debug!(
            "open(ino: {}, flags: {}, req: {})",
            _ino,
            _flags,
            _req.unique()
        );
        print_flags(&"open", _flags as i32);
        match self.db_mgr.lookup_by_ino(&self.mount_pt, _ino as i64) {
            None => {
                debug!("No entries found for ino: {}", _ino);
                reply.error(ENOENT);
            }
            Some(ent) => {
                let rw: bool = _flags as i32 & O_ACCMODE == O_RDWR;
                let wo: bool = _flags as i32 & O_ACCMODE == O_WRONLY;
                let ap: bool = _flags as i32 & O_APPEND == O_APPEND;

                if (rw || wo) && !ap {
                    debug!(
                        "open({}) - File opened for write, clearning data if exists",
                        ent.id,
                    );
                    self.db_mgr.clear_file_data(&ent.id);
                }

                self.fcache
                    .init(&self.mount_pt, ent.ino, ent.id, _flags, ent.segment_len);
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
        debug!(
            "read(ino = {}, fh = {}, offset = {}, size: {}, uid: {})",
            _ino,
            _fh,
            _offset,
            _size,
            _req.unique()
        );

        match self
            .fcache
            .get_cached(&self.mount_pt, _ino as i64, 0, &mut self.db_mgr)
        {
            None => {
                error!("File not cached, ino: {}", _ino);
                reply.error(ENOENT)
            }
            Some(fb) => {
                debug!("Cache found, ino: {}", fb.file_id);
                match fb.read(_offset, _size as i32, &mut self.db_mgr) {
                    Some(data) => reply.data(data.as_slice()),
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
        debug!(
            "write(ino: {}, offset: {}, data_len: {}, flags: {})",
            _ino,
            _offset,
            _data.len(),
            _flags
        );

        print_flags(&"write", _flags as i32);

        match self
            .fcache
            .get_cached(&self.mount_pt, _ino as i64, _flags, &mut self.db_mgr)
        {
            None => {
                error!("File not cached, ino: {}", _ino);
                reply.error(ENOENT)
            }
            Some(fb) => {
                debug!("Cache found, ino: {}", fb.file_id);
                fb.add(_offset, _data, &mut self.db_mgr);
                return reply.written(_data.len() as u32);
            }
        }
    }

    fn flush(&mut self, _req: &Request, _ino: u64, _fh: u64, _lock_owner: u64, reply: ReplyEmpty) {
        debug!("flush(ino: {} fh: {}, uid: {})", _ino, _fh, _req.unique());

        match self.db_mgr.lookup_by_ino(&self.mount_pt, _ino as i64) {
            None => {
                debug!("No entries found for ino: {}", _ino);
                reply.error(ENOENT);
            }
            Some(ent) => match self.fcache.remove(&self.mount_pt, &ent.ino) {
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
        debug!(
            "readdir(ino={}, fh={}, offset={}, mnt_pt: {}, uid: {})",
            ino,
            fh,
            offset,
            self.mount_pt,
            _req.uid()
        );
        let mut off = offset;
        let entries: Vec<db::Ent> = self
            .db_mgr
            .ls(self.mount_pt.to_string(), ino as i64, offset);
        debug!(
            "readdir(ino={}, fh={}, offset={}, mnt_pt: {}, num_files: {})",
            ino,
            fh,
            offset,
            self.mount_pt,
            entries.len()
        );

        for e in entries {
            off += 1;
            let fill = reply.add(
                e.ino as u64,
                off,
                if e.is_dir {
                    FileType::Directory
                } else {
                    FileType::RegularFile
                },
                &Path::new(&e.name),
            );
            if fill {
                break;
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

    debug!(
        "** {} Flags: [value - ro-rw-wo-ap]:[{} - {}-{}-{}-{}]",
        tag, flags, ro, rw, wo, ap
    );
}

pub fn mount(path: String, cfg_path: String) {
    let cfg: PgDbFsConfig = confy::load(&cfg_path).unwrap();
    let cfg_clone = cfg.clone();

    info!("Mounting pgdbfs on path: {}, config: {}", path, cfg_path);

    let mountpt = Path::new(&path);
    let mut db_mgr = PgDbMgr::new(cfg);
    db_mgr.init();

    let pgdbfs = PgDbFs {
        mount_pt: path.to_string(),
        db_mgr: db_mgr,
        fcache: fcache::FCache::new(),
        cfg: cfg_clone,
    };

    let result = fuse::mount(pgdbfs, &mountpt, &[]);
    match result {
        Ok(_r) => {
            debug!("Mounting pgdbfs on path: {}", path);
        }
        Err(err) => {
            panic!(err);
        }
    }
}
