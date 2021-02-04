extern crate chrono;
//extern crate postgres;
extern crate postgres;
extern crate r2d2;
extern crate r2d2_postgres;
extern crate time;

use self::r2d2::PooledConnection;

use self::r2d2_postgres::{postgres::NoTls, PostgresConnectionManager};

use self::time::Timespec;

use std::time::{Duration, UNIX_EPOCH};

use self::chrono::{DateTime, Utc};

use std::cmp;
use std::vec::Vec;

static TAG: &str = "db";

pub struct Ent {
    pub id: i64,
    pub name: String,
    pub is_dir: bool,
    pub ino: i64,
    pub size: i64,
    pub create_ts: DateTime<Utc>,
    pub update_ts: DateTime<Utc>,
}

pub struct EntData {
    pub ino: i64,
    pub data: Vec<u8>,
}

struct FsData {
    offset_st: i64,
    offset_en: i64,
    data: Vec<u8>,
}

#[derive(Debug)]
pub struct PgDbMgr {
    db_url: String,
    pool: Option<r2d2::Pool<PostgresConnectionManager<NoTls>>>,
}

pub fn make_a_pool(db_url: String) -> r2d2::Pool<PostgresConnectionManager<NoTls>> {
    //    let m = PostgresConnectionManager::new(db_url, TlsMode::None).unwrap();
    let host = format!(
        "host = {} user = {} dbname = {}",
        db_url, "pgdbfs", "pgdbfs"
    );
    let m = PostgresConnectionManager::new(host.parse().unwrap(), NoTls);

    let p = r2d2::Pool::new(m).unwrap();
    return p;
}

impl PgDbMgr {
    pub fn new(url: String) -> PgDbMgr {
        PgDbMgr {
            db_url: url,
            pool: None,
        }
    }

    pub fn init(&mut self) {
        let s = self.db_url.clone();

        let host = format!(
            "host = {} user = {} password = {} dbname = {}",
            self.db_url, "pgdbfs", "pgdbfs", "pgdbfs"
        );
        let cm = PostgresConnectionManager::new(host.parse().unwrap(), NoTls);
        println!("Connecting to : {}", host);
        //        let cm = PostgresConnectionManager::new(s, NoTls).unwrap();
        self.pool = Some(r2d2::Pool::new(cm).unwrap());
    }

    fn connect(&self) -> PooledConnection<PostgresConnectionManager<NoTls>> {
        match self.pool.as_ref() {
            None => {
                panic!("PgDbMgr not initialized, call init first")
            }
            Some(p) => {
                let conn = p.get().unwrap();
                return conn;
            }
        };
    }

    pub fn mkdir(&mut self, mnt_pt: &String, parent: i64, name: &str) {
        let sql = "insert into pgdbfs (id, mnt_pt, ino, parentid, name, size, is_dir) values ((select nextval('fsid_seq')), $1, (select nextval('ino_seq')), $2, $3, 4096, true)";
        let mut conn = self.connect();
        match conn.execute(sql, &[&mnt_pt, &parent, &name]) {
            Result::Ok(val) => {
                println!("$$$ {:?}", val)
            }
            Result::Err(err) => {
                panic!("mkdir failed: {:?}", err);
            }
        }
    }

    pub fn mkfile(&mut self, mnt_pt: &String, parent: i64, name: &str) {
        let sql = "insert into pgdbfs (id, mnt_pt, ino, parentid, name, size, is_dir) values ((select nextval('fsid_seq')), $1, (select nextval('ino_seq')), $2, $3, 0, false)";
        let mut conn = self.connect();
        match conn.execute(sql, &[&mnt_pt, &parent, &name]) {
            Result::Ok(val) => {
                println!("$$$ {:?}", val)
            }
            Result::Err(err) => {
                panic!("mkdir failed: {:?}", err);
            }
        }
    }

    pub fn setattr(
        &mut self,
        mnt_pt: &String,
        ino: i64,
        size: i64,
        create_ts: Timespec,
        update_ts: Timespec,
    ) -> u64 {
        let sql =
            "update pgdbfs set create_ts=$1, update_ts=$2, size=$3 where mnt_pt=$4 and ino=$5";
        let mut conn = self.connect();

        let cts: DateTime<Utc> =
            DateTime::<Utc>::from(UNIX_EPOCH + Duration::from_secs(create_ts.sec as u64));

        let uts: DateTime<Utc> =
            DateTime::<Utc>::from(UNIX_EPOCH + Duration::from_secs(update_ts.sec as u64));
        //        match conn.execute(sql, &[&create_ts, &update_ts, &size, &mnt_pt, &ino]) {
        match conn.execute(sql, &[&cts, &uts, &size, &mnt_pt, &ino]) {
            Result::Ok(val) => val,
            Result::Err(err) => {
                eprintln!(
                    "Failed to setattr for: {}, {}, reason: {}",
                    mnt_pt, ino, err
                );
                0
            }
        }
        //        return 0;
    }

    /// Looks up entry for the given mount point and parent inode and file name
    ///
    pub fn lookup(&mut self, mnt_pt: &String, ino: i64, name: &str) -> Option<Ent> {
        let mut conn = self.connect();
        let row_data = conn.query_one("select id, ino, name, is_dir, size, create_ts, update_ts from pgdbfs where mnt_pt=$1 and parentid=$2 and name=$3",
                               &[&mnt_pt, &ino, &name]);

        match row_data {
            Ok(row) => {
                let c: DateTime<chrono::offset::Utc> = row.get("create_ts");
                let u: DateTime<chrono::offset::Utc> = row.get("update_ts");
                let e = Ent {
                    id: row.get(0),
                    ino: row.get(1),
                    name: row.get(2),
                    is_dir: row.get(3),
                    size: row.get(4),
                    create_ts: c,
                    update_ts: u,
                };
                Some(e)
            }
            Err(err) => None,
        }
    }

    /// Looks up an entry by a specific inode number for the given mount point
    ///
    pub fn lookup_by_ino(&mut self, mnt_pt: &String, ino: i64) -> Option<Ent> {
        let mut conn = self.connect();
        let row_data = &conn.query_one("select id, ino, name, is_dir, size, create_ts, update_ts from pgdbfs where mnt_pt=$1 and ino=$2",
                               &[&mnt_pt, &ino]);

        match row_data {
            Ok(row) => {
                let c: DateTime<chrono::offset::Utc> = row.get("create_ts");
                let u: DateTime<chrono::offset::Utc> = row.get("update_ts");
                let e = Ent {
                    id: row.get(0),
                    ino: row.get(1),
                    name: row.get(2),
                    is_dir: row.get(3),
                    size: row.get(4),
                    create_ts: c,
                    update_ts: u,
                };
                Some(e)
            }
            Err(err) => None,
        }
    }

    pub fn ls(&mut self, mnt_pt: String, ino: i64) -> Vec<Ent> {
        let mut conn = self.connect();
        let mut v: Vec<Ent> = Vec::new();
        println!("** {} - ls: {}, {}", TAG, mnt_pt, ino);
        for row in &conn.query("select id, name, is_dir, ino, size, create_ts, update_ts from pgdbfs where mnt_pt=$1 and parentid=$2", &[&mnt_pt, &ino]).unwrap() {

            let c: DateTime<chrono::offset::Utc> = row.get("create_ts");
            let u: DateTime<chrono::offset::Utc> = row.get("update_ts");
            let e = Ent {
                id: row.get(0),
                name: row.get(1),
                is_dir: row.get(2),
                ino: row.get(3),
                size: row.get(4),
                create_ts: c,
                update_ts: u,
            };
            v.push(e)
        }
        println!("** {} - ls found: {} entries", TAG, v.len());
        v
    }

    pub fn read(&mut self, id: i64, offset: i64, size: u32) -> Option<Vec<u8>> {
        let mut conn = self.connect();

        //let offset_end = offset + size as i64;
        let offset_end = offset;

        let sql = "select file_offset_st, file_offset_en, data from pgdbfs_data where fsid=$1 and $2 between file_offset_st and file_offset_en order by file_offset_st";

        println!(
            "** {} - read (id: {}, offset_st: {}, offset_en: {}, {})",
            TAG, id, offset, offset_end, sql
        );

        let mut fs_data: Vec<u8> = Vec::new();

        let mut row_offset_min: i64 = i64::MAX;
        let mut row_offset_max: i64 = i64::MIN;
        for row in &conn.query(sql, &[&id, &offset_end]).unwrap() {
            let d: Vec<u8> = row.get(2);
            let o: i64 = row.get(0);
            let e: i64 = row.get(1);
            row_offset_min = cmp::min(row_offset_min, o);
            println!("??? {} {} {}", o, e, row_offset_min);
            fs_data.extend(d.iter().copied());
        }

        if (fs_data.len() == 0) {
            Some(fs_data)
        } else {
            let slice_st = (offset - row_offset_min) as usize;
            let mut slice_en = (slice_st + size as usize) as usize;
            if slice_en > fs_data.len() {
                slice_en = fs_data.len();
            }
            println!(
                "** {} read - initial_offset: {}, total_fs_len: {} slice_st: {} slice_en: {}",
                TAG,
                offset,
                fs_data.len(),
                slice_st,
                slice_en
            );
            Some(
                fs_data
                    .drain(slice_st as usize..slice_en as usize)
                    .collect(),
            )
        }
    }

    pub fn write(
        &mut self,
        mnt_pt: &String,
        ino: i64,
        offset_st: i64,
        offset_en: i64,
        data: &Vec<u8>,
    ) -> u64 {
        let mut conn = self.connect();

        match self.lookup_by_ino(mnt_pt, ino) {
            None => {
                println!("** {} - No entries found for ino: {}", TAG, ino);
                0
            }
            Some(ent) => {
                let sql = "insert into pgdbfs_data (id, fsid, file_offset_st, file_offset_en, data) values
                           ( (select nextval('fsid_seq')), $1, $2, $3, $4)";
                println!(
                    "Inserting data for fsid: {}, offset: {}, offset_en: {}, len: {}",
                    ent.id,
                    offset_st,
                    offset_en,
                    data.len()
                );
                match conn.execute(sql, &[&ent.id, &offset_st, &offset_en, &data]) {
                    Result::Ok(val) => {
                        self.update_file_sz(ent.id, offset_en);
                        val
                    }
                    Result::Err(err) => {
                        eprintln!("Failed to write for: {}, {}, reason: {}", mnt_pt, ino, err);
                        0
                    }
                }
            }
        }
        //return 0;
    }

    fn update_file_sz(&mut self, id: i64, fsize: i64) -> bool {
        let mut conn = self.connect();

        let sql = "update pgdbfs set size=$1 where id=$2";
        println!("Updating file size for fsid: {}, fsize: {}", id, fsize);
        match conn.execute(sql, &[&fsize, &id]) {
            Result::Ok(val) => true,
            Result::Err(err) => false,
        }
    }
}
