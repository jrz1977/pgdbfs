extern crate postgres;
extern crate time;
extern crate r2d2;
extern crate r2d2_postgres;

use self::postgres::{Connection};
use self::time::{Timespec,Tm};
use std::vec::Vec;
use self::r2d2::{Pool, PooledConnection};
use self::r2d2_postgres::{TlsMode, PostgresConnectionManager};

static TAG: &str = "db";

pub struct Ent {
    pub name: String,
    pub is_dir: bool,
    pub ino: i64,
    pub size: i64,
    pub create_ts: Timespec,
    pub update_ts: Timespec
}

#[derive(Debug)]
pub struct PgDbMgr {
    db_url: String,
    pool: Option<r2d2::Pool<PostgresConnectionManager>>,
}

pub fn make_a_pool(db_url: String) -> r2d2::Pool<PostgresConnectionManager> {
    let m = PostgresConnectionManager::new(db_url, TlsMode::None).unwrap();
    let p = r2d2::Pool::new(m).unwrap();
    return p;
}

impl PgDbMgr {
    pub fn new(db_url: String) -> PgDbMgr {
        PgDbMgr {
            db_url: db_url,
            pool: None,
        }
    }

    pub fn init(&mut self) {
        let s = self.db_url.clone();
        let cm = PostgresConnectionManager::new(s, TlsMode::None).unwrap();  
        self.pool = Some(r2d2::Pool::new(cm).unwrap());
    }
    
    fn connect(&self) -> PooledConnection<PostgresConnectionManager> {
        match self.pool.as_ref() {
            None => { panic!("PgDbMgr not initialized, call init first") },
            Some(p) => {
                let conn = p.get().unwrap();
                return conn;                
            }
        };
    }

    pub fn mkdir(&mut self, mnt_pt: &String, parent: i64, name: &str) {
        let sql = "insert into pgdbfs (mnt_pt, ino, parentid, name, size, is_dir) values ($1, (select nextval('ino_seq')), $2, $3, 4096, true)";
        let conn = self.connect();
        match conn.execute(sql, &[&mnt_pt, &parent, &name]) {
            Result::Ok(val) => { println!("$$$ {:?}", val) },
            Result::Err(err) => {
                panic!("mkdir failed: {:?}", err);
            }
        }
    }
    
    pub fn mkfile(&mut self, mnt_pt: &String, parent: i64, name: &str) {
        let sql = "insert into pgdbfs (mnt_pt, ino, parentid, name, size, is_dir) values ($1, (select nextval('ino_seq')), $2, $3, 0, false)";
        let conn = self.connect();
        match conn.execute(sql, &[&mnt_pt, &parent, &name]) {
            Result::Ok(val) => { println!("$$$ {:?}", val) },
            Result::Err(err) => {
                panic!("mkdir failed: {:?}", err);
            }
        }
    }
    
    pub fn setattr(&mut self, mnt_pt: &String, ino: i64, size: i64, create_ts: Timespec, update_ts: Timespec) -> u64 {
        let sql = "update pgdbfs set create_ts=$1, update_ts=$2, size=$3 where mnt_pt=$4 and ino=$5";
        let conn = self.connect();
        let cts:Timespec = time::now_utc().to_timespec();
        match conn.execute(sql, &[&create_ts, &update_ts, &size, &mnt_pt, &ino]) {
            Result::Ok(val) => { return val; },
            Result::Err(err) => {
                eprintln!("Failed to setattr for: {}, {}, reason: {}", mnt_pt, ino, err);
                return 0;
            }
        }
        return 0;
    }
    
    
    /// Looks up entry for the given mount point and parent inode and file name
    ///
    pub fn lookup(&mut self, mnt_pt: &String, ino: i64, name: &str) -> Option<Ent> {
        println!("** {} - lookup called for: mnt_pt: {}, ino: {}, name: {}", TAG, mnt_pt, ino, name);
        let conn = self.connect();
        for row in &conn.query("select ino, name, is_dir, size, create_ts, update_ts from pgdbfs where mnt_pt=$1 and parentid=$2 and name=$3",
                               &[&mnt_pt, &ino, &name]).unwrap() {
            let e = Ent {
                ino: row.get(0),
                name: row.get(1),
                is_dir: row.get(2),
                size: row.get(3),
                create_ts: row.get(4),
                update_ts: row.get(5),
            };
            return Some(e)
        }
        return None;
    }
    
    /// Looks up an entry by a specific inode number for the given mount point
    ///
    pub fn lookup_by_ino(&mut self, mnt_pt: &String, ino: i64) -> Option<Ent> {
        println!("** {} - lookup called for: {}", TAG, ino);
        let conn = self.connect();
        for row in &conn.query("select ino, name, is_dir, size, create_ts, update_ts from pgdbfs where mnt_pt=$1 and ino=$2",
                               &[&mnt_pt, &ino]).unwrap() {
            let e = Ent {
                ino: row.get(0),
                name: row.get(1),
                is_dir: row.get(2),
                size: row.get(3),
                create_ts: row.get(4),
                update_ts: row.get(5),
            };
            return Some(e)
        }
        return None;
    }
    
    pub fn ls(&mut self, mnt_pt: String, ino: i64) -> Vec<Ent> {
        let conn = self.connect();
        let mut v: Vec<Ent> = Vec::new();
        println!("** {} - ls: {}, {}", TAG, mnt_pt, ino);
        for row in &conn.query("select name, is_dir, ino, size, create_ts, update_ts from pgdbfs where mnt_pt=$1 and parentid=$2", &[&mnt_pt, &ino]).unwrap() {
            let e = Ent {
                name: row.get(0),
                is_dir: row.get(1),
                ino: row.get(2),
                size: row.get(3),
                create_ts: row.get(4),
                update_ts: row.get(5),
            };
            v.push(e)
        }
        println!("** {} - ls found: {} entries", TAG, v.len());
        return v;
    }
    
    pub fn write(&mut self, mnt_pt: &String, ino: i64, data: &[u8]) -> u64 {
        let conn = self.connect();
        let sql = "update pgdbfs set data=$1, size=$2 where mnt_pt=$3 and ino=$4";
        let len: i64 = data.len() as i64;
        match conn.execute(sql, &[&data, &len, &mnt_pt, &ino]) {
            Result::Ok(val) => { return val; },
            Result::Err(err) => {
                eprintln!("Failed to write for: {}, {}, reason: {}", mnt_pt, ino, err);
                return 0;
            }
        }
        return 0;    
    }    
}
