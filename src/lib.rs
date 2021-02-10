#[macro_use]
extern crate log;
extern crate ctrlc;
extern crate serde;

pub mod db;
pub mod fcache;
pub mod fsys;

extern crate lazy_static;

#[cfg(test)]
mod tests {
    use db;
    use db::Ent;
    use fsys;
    use std::fs;

    // #[test]
    // fn test_db_conn() {
    //     let entries:Vec<db::Ent> = db::ls("/tmp/pgdbfs".to_string(), 1);
    //     for e in entries {
    //         println!("Entry: {}", e.ino);
    //     }
    // }

    #[test]
    fn test_fs() {
        let path = String::from("/tmp/pgdbfs");
        fsys::mount(path);
    }

    fn test_fcache() {
        let mut db_mgr = PgDbMgr::new(String::from("localhost"));
        db_mgr.init();

        let mut fc = fcache::FCache::new();

        fc.init(1);

        println!("FileCache: {}", fc);

        let mut fb = fcache::FBuffer::new(1, 1000);

        let mut c: u8 = 0;
        for i in 0..4 {
            let mut v: Vec<u8> = Vec::new();
            for j in 0..8 {
                c += 1;
                v.push(c);
            }
            fb.add(&v, &db_mgr);
            println!("{} -- {:?}", i, v);
        }

        println!("FBuffer: {:?}", fb);

        println!("FBuffer: {}", fb);
        let off: i64 = 1;
        let len: i32 = 10;
        let segments = fb.get_segment_indexes(off, len);

        let read = fb.read(off, len);
        println!(
            "$$$$ off: {}, len: {}, indexes: {:?}",
            off,
            len,
            segments.unwrap()
        );
        println!("Read data: {:?}", read);
    }
}
