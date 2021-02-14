#[macro_use]
extern crate log;
extern crate clap;

pub mod db;
pub mod fcache;
pub mod fsys;

use clap::{App, Arg};
use std::path::Path;
use std::process::Command;

extern crate serde;

fn main() {
    env_logger::init();
    let home = dirs::home_dir().unwrap();
    let cfg_path = format!("{}/.pgdbfs/pgdbfs", home.to_str().unwrap());

    let matches = App::new("PgDbFs")
        .version("0.1.0")
        .author("https://github.com/jrz1977/pgdbfs")
        .about("PostgreSQL backed FUSE File System")
        .arg(
            Arg::with_name("mount-pt")
                .short("m")
                .long("mount-pt")
                .takes_value(true)
                .required(true)
                .help("Mount point"),
        )
        .arg(
            Arg::with_name("config-file")
                .short("f")
                .long("config-file")
                .takes_value(true)
                .help("Config file path"),
        )
        .get_matches();

    let cfg_path_path = Path::new(matches.value_of("config-file").unwrap_or(&cfg_path));

    let normalized_config_file_path = get_normalized_cfg_path(&cfg_path_path);

    let mnt_pt = matches.value_of("mount-pt").unwrap();

    if Path::new(mnt_pt).exists() {
        let umount_cmd = format!("fusermount -u {}", mnt_pt);

        ctrlc::set_handler(move || {
            info!("Ctrl-C: Unmounting file system");

            Command::new("sh")
                .arg("-c")
                .arg(&umount_cmd)
                .output()
                .expect("");

            std::process::exit(0);
        })
        .expect("Error setting Ctrl-C handler");

        fsys::mount(mnt_pt.to_string(), normalized_config_file_path);
    } else {
        error!("Path: {} does not exist", mnt_pt);
    }
}

// Returns config file with extension stripped out if exists
fn get_normalized_cfg_path(cfg_path: &Path) -> String {
    let path_buf = cfg_path.with_extension("");
    String::from(path_buf.as_path().to_str().unwrap())
}
