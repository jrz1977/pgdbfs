pub mod db;
pub mod fsys;
#[macro_use]
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
}
