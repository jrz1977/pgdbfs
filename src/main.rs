pub mod db;
pub mod fcache;
pub mod fsys;

use db::PgDbMgr;

fn main() {
    println!("Hello, world!");

    let path = String::from("/tmp/pgdbfs");
    fsys::mount(path);

    /*
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
    println!("Read data: {:?}", read);*/
}
