extern crate fuse;
extern crate libc;
extern crate time;
extern crate rand;

use std::path::Path;
use std::ffi::OsStr;
use std::mem;
use std::os;

use self::libc::{ENOENT, ENOSYS,EIO,ENOTEMPTY};
use self::time::{Timespec, Tm};
use self::fuse::{FileAttr, FileType, Filesystem, Request, ReplyAttr, ReplyEntry, ReplyEmpty, ReplyDirectory, ReplyOpen, ReplyWrite};

use db;
use db::{Ent, PgDbMgr};


static TAG: &str = "fsys";

#[derive(Debug)]
pub struct PgDbFs {
    mount_pt: String,
    db_mgr: PgDbMgr,
}

pub trait DbFsUtils {
    
    fn make_file_entry(&self, ent: &db::Ent) -> FileAttr {
        let ttl = Timespec::new(1, 0);
        let attr = FileAttr {
            ino: ent.ino as u64,
            size: ent.size as u64,
            blocks: 0,
            atime: ent.create_ts,
            mtime: ent.update_ts,
            ctime: ent.update_ts,
            crtime: ent.create_ts,
            kind: if ent.is_dir { FileType::Directory} else { FileType::RegularFile },
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

impl DbFsUtils for PgDbFs {
}

impl Filesystem for PgDbFs {

    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        println!("** {} - lookup(parent={}, name={:?})", TAG, parent, name.to_str());
        match name.to_str() {
            None => {
                println!("No value in name, parent: {}", parent);
                return;
            }
            Some(n) => {
                let nameStr = String::from(n);
                match self.db_mgr.lookup(&self.mount_pt, parent as i64, n) {
                    None => {
                        println!("No entries found for parent: {}, name: {:?}", parent, name);
                        reply.error(ENOENT);                
                    },
                    Some(ent) => {
                        let ts = time::now().to_timespec();
                        let ttl = Timespec::new(1, 0);
                        let attr = self.make_file_entry(&ent);
                        reply.entry(&ttl, &attr, 0);
                    }
                }
            }
        }
    }

    //
    fn readdir(&mut self, _req: &Request, ino: u64, fh: u64, offset: i64, mut reply: ReplyDirectory) {
        println!("** {} - readdir(ino={}, fh={}, offset={}, mnt_pt: {})", TAG, ino, fh, offset, self.mount_pt);
        let mut off = 0;
        if offset == 0 {
            let entries:Vec<db::Ent> = self.db_mgr.ls(&self.mount_pt, ino as i64);            
            reply.add(ino, off+1, FileType::Directory, &Path::new("."));
            reply.add(ino, off+1, FileType::Directory, &Path::new(".."));
            
            for e in entries {
                //println!("** {} - Adding ent: {}", TAG, e.name);
                reply.add(e.ino as u64,
                          off+1,
                          if e.is_dir { FileType::Directory } else { FileType::RegularFile },
                          &Path::new(&e.name));
            }        
        }
        reply.ok();
    }
    //
    
    fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
        println!("** {} - getattr(ino={})", TAG, ino);

        match self.db_mgr.lookup_by_ino(&self.mount_pt, ino as i64) {
            None => println!("** {} - No entries found for ino: {}", TAG, ino),
            Some(ent) => {
                let attr = self.make_file_entry(&ent);
                reply.attr(&ent.create_ts, &attr);
            }
        }
    }

    fn mkdir(&mut self, _req: &Request, parent: u64, name: &OsStr, mode: u32, reply: ReplyEntry) {
        println!("** {} - mkdir(parent: {}, name: {:?}", TAG, parent, name.to_str());
        self.db_mgr.mkdir(&self.mount_pt, parent as i64, &name.to_str().unwrap());
        match self.db_mgr.lookup(&self.mount_pt, parent as i64, &name.to_str().unwrap()) {
            None => panic!("Failed to lookup created dir"),
            Some(ent) => {
                let attr = self.make_file_entry(&ent);
                reply.entry(&ent.create_ts, &attr, 0);
            }
        }        
    }

    fn mknod(&mut self, _req: &Request, parent: u64, name: &OsStr, mode: u32, rdev: u32, reply: ReplyEntry) {
        println!("** {} - mknod (parent: {}, mode: {}, rdev: {}, name: {:?}",
                 TAG,
                 parent,
                 mode,
                 rdev,
                 name.to_str());

        self.db_mgr.mkfile(&self.mount_pt, parent as i64, &name.to_str().unwrap());
        match self.db_mgr.lookup(&self.mount_pt, parent as i64, &name.to_str().unwrap()) {
            None => panic!("Failed to lookup created dir"),
            Some(ent) => {
                let attr = self.make_file_entry(&ent);
                reply.entry(&ent.create_ts, &attr, 0);
            }
        }                
    }

    fn setattr(&mut self, _req: &Request, ino: u64, mode: Option<u32>, uid: Option<u32>, gid: Option<u32>, size: Option<u64>, atime: Option<Timespec>, mtime: Option<Timespec>, fh: Option<u64>, crtime: Option<Timespec>, chgtime: Option<Timespec>, bkuptime: Option<Timespec>, flags: Option<u32>, reply: ReplyAttr) {

        println!("** {} - setattr (ino: {}, mode: {:?}, size: {:?}, at: {:?}, mt: {:?}",
                 TAG,
                 ino,
                 mode,
                 size,
                 atime,
                 mtime
        );
        let cts: Timespec = time::now_utc().to_timespec();
        let cr_tm = match atime {
            Some(val) => val,
            _ => cts
        };
        let up_tm = match mtime {
            Some(val) => val,
            _ => cts
        };
        let sz = match size {
            Some(val) => val,
            _ => 0
        };
        let updt_count = self.db_mgr.setattr(&self.mount_pt, ino as i64, sz as i64, cr_tm, up_tm);

        println!("** {}, setattr(DB update count: {}, for: {}, {}", TAG, updt_count, self.mount_pt, ino);

        match self.db_mgr.lookup_by_ino(&self.mount_pt, ino as i64) {
            None => panic!("Failed to lookup created dir"),
            Some(ent) => {
                let attr = self.make_file_entry(&ent);
                reply.attr(&ent.create_ts, &attr);
            }
        }
    }

    fn open(&mut self, _req: &Request, ino: u64, flags: u32, reply: ReplyOpen) {
        println!("** {} - open(ino: {}, flags: {})", TAG, ino, flags);
        reply.opened(0, flags);
    }

    fn write(&mut self, _req: &Request, ino: u64, fh: u64, offset: i64, data: &[u8], flags: u32, reply: ReplyWrite) {
        println!("** {} - write(ino: {}, offset: {}, data_len: {})", TAG, ino, offset, data.len());

        match self.db_mgr.write(&self.mount_pt, ino as i64, data) {
            1 => {
                println!("** {}, write(DB update count: {}, for: {}, {}",
                         TAG, 1, self.mount_pt, ino);
                
                reply.written(data.len() as u32);
            },
            _ => reply.error(ENOSYS)
        }
    }

    fn unlink(&mut self, _req: &Request, _parent: u64, _name: &OsStr, reply: ReplyEmpty) {
        println!("** {} - unlink(parent: {}, name: {:?})", TAG, _parent, _name);
        let str = _name.to_str().unwrap();
        match self.db_mgr.delete_file(&self.mount_pt, _parent as i64, &str) {
            1 => {
                println!("** {}, unlink success: {}, {:?}", TAG, _parent, _name);
                reply.ok()
            },
            _ => reply.error(EIO)
        }
    }

    fn rmdir(
        &mut self, 
        _req: &Request, 
        _parent: u64, 
        _name: &OsStr, 
        reply: ReplyEmpty
    ) {
        println!("** {} - rmdir(parent: {}, name: {:?})", TAG, _parent, _name);
        let str = _name.to_str().unwrap();
        match self.db_mgr.lookup(&self.mount_pt, _parent as i64, &str) {
            Some(ent) => {
                let self_ino = ent.ino;
                match self.db_mgr.num_children(&self.mount_pt, self_ino) {
                    0 => {
                        println!("Now remove directory: {}", str);
                        match self.db_mgr.delete_file(&self.mount_pt, _parent as i64, &str) {
                            1 => {
                                reply.ok()
                            }
                            _ => reply.error(EIO)
                        }
                    }
                    _ => {
                        reply.error(ENOTEMPTY)
                    }
                }
            },
            None => {
            }
        }
    }
}


pub fn mount(path: String) {
    println!("** {}, Mounting pgdbfs on path: {}", TAG, path);
    let mountpt = Path::new(&path);
    let mut db_mgr = PgDbMgr::new(String::from("postgres://pgdbfs:pgdbfs@localhost/pgdbfs"));
    db_mgr.init();
    
    let pgdbfs = PgDbFs {
        mount_pt: path.to_string(),
        db_mgr: db_mgr,
    };
    
    fuse::mount(pgdbfs, &mountpt, &[]);
}
