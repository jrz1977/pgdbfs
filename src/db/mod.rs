extern crate chrono;
extern crate postgres;
extern crate r2d2;
extern crate r2d2_postgres;
extern crate time;

use self::r2d2::PooledConnection;

use self::r2d2_postgres::{postgres::NoTls, PostgresConnectionManager};

use self::time::Timespec;

use std::time::{Duration, UNIX_EPOCH};

use self::chrono::{DateTime, Utc};

use std::vec::Vec;

use fsys::PgDbFsConfig;

pub struct Ent {
    pub id: i64,
    pub name: String,
    pub is_dir: bool,
    pub ino: i64,
    pub size: i64,
    pub segment_len: i32,
    pub create_ts: DateTime<Utc>,
    pub update_ts: DateTime<Utc>,
    pub nlink: i64,
}

pub struct EntData {
    pub ino: i64,
    pub data: Vec<u8>,
}

#[derive(Debug)]
pub struct PgDbMgr {
    cfg: PgDbFsConfig,
    pool: Option<r2d2::Pool<PostgresConnectionManager<NoTls>>>,
}

impl PgDbMgr {
    pub fn new(cfg: PgDbFsConfig) -> PgDbMgr {
        PgDbMgr {
            cfg: cfg,
            pool: None,
        }
    }

    pub fn init(&mut self) {
        let host = format!(
            "host = {} user = {} password = {} dbname = {}",
            self.cfg.db_host, self.cfg.db_user, self.cfg.db_pass, self.cfg.db_user
        );
        let cm = PostgresConnectionManager::new(host.parse().unwrap(), NoTls);
        debug!("Connecting to : {}", host);
        self.pool = Some(r2d2::Pool::builder().max_size(15).build(cm).unwrap());
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
        let sql = "insert into pgdbfs (id, mnt_pt, ino, parentid, name, size, segment_len, is_dir) values ((select nextval('fsid_seq')), $1, (select nextval('ino_seq')), $2, $3, 4096, 0, true)";
        let mut conn = self.connect();
        match conn.execute(sql, &[&mnt_pt, &parent, &name]) {
            Result::Ok(val) => {
                debug!("$$$ {:?}", val)
            }
            Result::Err(err) => {
                panic!("mkdir failed: {:?}", err);
            }
        }
    }

    pub fn mkfile(&mut self, mnt_pt: &String, parent: i64, name: &str, segment_len: &i32) {
        let sql = "insert into pgdbfs (id, mnt_pt, ino, parentid, name, size, segment_len, is_dir) values ((select nextval('fsid_seq')), $1, (select nextval('ino_seq')), $2, $3, 0, $4, false)";
        let mut conn = self.connect();
        match conn.execute(sql, &[&mnt_pt, &parent, &name, segment_len]) {
            Result::Ok(val) => {
                debug!("$$$ {:?}", val)
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

        match conn.execute(sql, &[&cts, &uts, &size, &mnt_pt, &ino]) {
            Result::Ok(val) => val,
            Result::Err(err) => {
                debug!(
                    "Failed to setattr for: {}, {}, reason: {}",
                    mnt_pt, ino, err
                );
                0
            }
        }
    }

    /// Looks up entry for the given mount point and parent inode and file name
    ///
    pub fn lookup(&mut self, mnt_pt: &String, ino: i64, name: &str) -> Option<Ent> {
        let mut conn = self.connect();

        let sql = "select p.*, (select count(*)::int8 from pgdbfs where parentid=p.ino and is_dir=true) as child_count from pgdbfs p where mnt_pt=$1 and parentid=$2 and name=$3";

        debug!("lookup(sql: {}", sql);

        let row_data = conn.query_one(sql, &[&mnt_pt, &ino, &name]);

        match row_data {
            Ok(row) => {
                let c: DateTime<chrono::offset::Utc> = row.get("create_ts");
                let u: DateTime<chrono::offset::Utc> = row.get("update_ts");
                let e = Ent {
                    id: row.get("id"),
                    ino: row.get("ino"),
                    name: row.get("name"),
                    is_dir: row.get("is_dir"),
                    size: row.get("size"),
                    segment_len: row.get("segment_len"),
                    create_ts: c,
                    update_ts: u,
                    nlink: row.get("child_count"),
                };
                Some(e)
            }
            Err(_err) => None,
        }
    }

    /// Looks up an entry by a specific inode number for the given mount point
    ///
    pub fn lookup_by_ino(&mut self, mnt_pt: &String, ino: i64) -> Option<Ent> {
        let mut conn = self.connect();

        let sql = "select p.*, (select count(*)::int8 from pgdbfs where parentid=p.ino and is_dir=true) as child_count from pgdbfs p where mnt_pt=$1 and ino=$2";

        debug!(
            "lookup_by_ino(sql: {}, mnt_pt: {}, ino: {})",
            sql, mnt_pt, ino
        );

        let row_data = &conn.query_one(sql, &[&mnt_pt, &ino]);

        match row_data {
            Ok(row) => {
                let c: DateTime<chrono::offset::Utc> = row.get("create_ts");
                let u: DateTime<chrono::offset::Utc> = row.get("update_ts");
                let e = Ent {
                    id: row.get("id"),
                    ino: row.get("ino"),
                    name: row.get("name"),
                    is_dir: row.get("is_dir"),
                    size: row.get("size"),
                    segment_len: row.get("segment_len"),
                    create_ts: c,
                    update_ts: u,
                    nlink: row.get("child_count"),
                };
                debug!(
                    "lookup_by_ino(mnt: {}, ino: {}, id: {}, name: {}, sz: {}",
                    mnt_pt, ino, e.id, e.name, e.size
                );
                Some(e)
            }
            Err(_err) => None,
        }
    }

    pub fn ls(&mut self, mnt_pt: String, ino: i64, offset: i64) -> Vec<Ent> {
        let mut conn = self.connect();
        let mut v: Vec<Ent> = Vec::new();
        let sql = "select id, name, is_dir, ino, size, segment_len, create_ts, update_ts from pgdbfs where mnt_pt=$1 and parentid=$2 order by id offset $3 limit 100";

        for row in &conn.query(sql, &[&mnt_pt, &ino, &offset]).unwrap() {
            let c: DateTime<chrono::offset::Utc> = row.get("create_ts");
            let u: DateTime<chrono::offset::Utc> = row.get("update_ts");
            let e = Ent {
                id: row.get(0),
                name: row.get(1),
                is_dir: row.get(2),
                ino: row.get(3),
                size: row.get(4),
                segment_len: row.get(5),
                create_ts: c,
                update_ts: u,
                nlink: 0,
            };
            v.push(e)
        }
        debug!("ls found: {} entries", v.len());
        v
    }

    pub fn load_segment(&mut self, file_id: &i64, segment_no: &i64) -> Option<Vec<u8>> {
        let mut conn = self.connect();
        let sql = "select m.id, m.size, m.is_dir, d.data, d.segment_no from pgdbfs m left join 
                (select data, fsid, segment_no from pgdbfs_data where fsid=$1 and segment_no=$2) d 
                on m.id=d.fsid where m.id=$1";

        debug!(
            "load_segment(file_id: {}, segment_no: {}, sql: {})",
            file_id, segment_no, sql
        );
        let row_data = conn.query_one(sql, &[file_id, segment_no]);
        match row_data {
            Ok(row) => {
                let sz: i64 = row.get("size");
                if sz == 0 {
                    Some(Vec::new())
                } else {
                    Some(row.get("data"))
                }
            }
            Err(_err) => {
                error!("Failed sql {}", _err);
                None
            }
        }
    }

    pub fn writep(&mut self, file_id: &i64, segment_no: &i64, data: &Vec<u8>) -> u64 {
        let mut conn = self.connect();

        let sql = "insert into pgdbfs_data (id, fsid, segment_no, data) values
                           ( (select nextval('fsid_seq')), $1, $2, $3) on conflict on constraint pgdbfs_data_uk do update set data=$3";

        match conn.execute(sql, &[&file_id, &segment_no, &data]) {
            Result::Ok(val) => {
                self.update_file_sz(file_id, data.len() as i64);
                val
            }
            Result::Err(err) => {
                debug!("Failed to write for file_id: {}, reason: {}", file_id, err);
                0
            }
        }
    }

    pub fn check_segment_exists(&mut self, file_id: &i64, segment_no: &i64) -> bool {
        let mut conn = self.connect();
        let sql = "select count(*)::int as cnt from pgdbfs_data where fsid=$1 and segment_no=$2";

        let row_data = conn.query_one(sql, &[file_id, segment_no]);
        match row_data {
            Ok(row) => {
                let count: i32 = row.get("cnt");
                if count == 0 {
                    return false;
                }
                true
            }
            Err(_err) => false,
        }
    }

    pub fn clear_file_data(&mut self, file_id: &i64) -> bool {
        let mut conn = self.connect();
        let sql = "delete from pgdbfs_data where fsid=$1";
        debug!("clear_data_for_file(file_id: {})", file_id);
        match conn.execute(sql, &[file_id]) {
            Result::Ok(_val) => true,
            Result::Err(_err) => false,
        }
    }

    fn update_file_sz(&mut self, id: &i64, fsize: i64) -> bool {
        let mut conn = self.connect();

        let sql = "update pgdbfs set size=size + $1 where id=$2";

        match conn.execute(sql, &[&fsize, &id]) {
            Result::Ok(_val) => true,
            Result::Err(_err) => false,
        }
    }

    pub fn has_children(&mut self, file_id: &i64) -> bool {
        let mut conn = self.connect();

        let sql = "select count(*)::int as cnt from pgdbfs where parentid=$1";

        let row_data = conn.query_one(sql, &[file_id]);
        match row_data {
            Ok(row) => {
                let count: i32 = row.get("cnt");
                debug!("has_children(file_id: {}, cnt: {})", file_id, count);

                if count == 0 {
                    return false;
                }
                true
            }
            Err(_err) => false,
        }
    }
    /*
    pub fn num_children(&mut self, file_id: &i64, children_type: Option<fuse::FileType>) -> bool {
        let mut conn = self.connect();

        let mut sql = "select count(*)::int as cnt from pgdbfs where parentid=$1";
        let mut params = [];
        match children_type {
            None => (),
            fuse::FileType::Directory => {
                sql = "select count(*)::int as cnt from pgdbfs parentid=$1 and is_dir=true";
            }
            fuse::FileType::RegularFile => {
                sql = "select count(*)::int as cnt from pgdbfs where parent_id=$1 and is_dir=false";
            }
        }

        let row_data = conn.query_one(sql, &[file_id]);
        match row_data {
            Ok(row) => {
                let count: i32 = row.get("cnt");
                debug!("has_children(file_id: {}, cnt: {})", file_id, count);

                if count == 0 {
                    return false;
                }
                true
            }
            Err(_err) => false,
        }
    }*/

    pub fn update_parent(&mut self, file_id: &i64, parent_id: &i64) -> bool {
        let mut conn = self.connect();

        let sql = "update pgdbfs set parentid=$1 where id=$2";

        let updt_cnt = conn.execute(sql, &[parent_id, file_id]).unwrap();

        return updt_cnt == 1;
    }

    pub fn update_name(&mut self, file_id: &i64, name: &str) -> bool {
        let mut conn = self.connect();

        let sql = "update pgdbfs set name=$1 where id=$2";

        let updt_cnt = conn.execute(sql, &[&name, file_id]).unwrap();

        return updt_cnt == 1;
    }

    pub fn delete_entity(&mut self, file_id: &i64) -> u64 {
        let mut conn = self.connect();

        let sql = "delete from pgdbfs where id=$1";

        let updt_cnt = conn.execute(sql, &[file_id]).unwrap();

        debug!("delete_entity(file_id: {})", file_id);

        return updt_cnt;
    }

    pub fn get_file_sz(&mut self, file_id: &i64) -> i64 {
        let mut conn = self.connect();

        let sql = "select size from pgdbfs where id=$1";

        let row_data = conn.query_one(sql, &[file_id]);
        match row_data {
            Ok(row) => {
                let count: i64 = row.get("size");
                return count;
            }
            Err(_err) => -1,
        }
    }
}
