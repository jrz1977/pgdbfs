#[macro_use]
extern crate log;

pub mod db;
pub mod fcache;
pub mod fsys;

use std::env;
use std::path::Path;
use std::process::Command;

extern crate serde;

fn main() {
    env_logger::init();

    let args: Vec<String> = env::args().collect();

    if args.len() != 2 {
        println!("usage: cargo run [mount point]");
        std::process::exit(1);
    }

    let mnt_pt = &args[1];

    if Path::new(mnt_pt).exists() {
        let umount_cmd = format!("fusermount -u {}", mnt_pt);

        ctrlc::set_handler(move || {
            error!("Ctrl-C: Unmounting file system");

            Command::new("sh")
                .arg("-c")
                .arg(&umount_cmd)
                .output()
                .expect("");

            std::process::exit(0);
        })
        .expect("Error setting Ctrl-C handler");

        fsys::mount(mnt_pt.to_string());
    } else {
        error!("Path: {} does not exist", mnt_pt);
    }
}
