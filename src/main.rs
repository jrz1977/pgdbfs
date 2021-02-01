pub mod db;
pub mod fsys;

fn main() {
    println!("Hello, world!");

    let path = String::from("/tmp/pgdbfs");
    fsys::mount(path);
}
