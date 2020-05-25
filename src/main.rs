use clap::{App, Arg};
use md5;
use rayon::prelude::*;
use std::fs::File;
use std::io::Read;

fn main() {
    let matches = App::new("s3etag")
        .version("1.0")
        .about("It compares a file checksum to an s3 e-tag")
        .arg(
            Arg::with_name("filepath")
                .short("f")
                .long("filepath")
                .value_name("FILE")
                .help("The path of the file you want to check")
                .takes_value(true)
                .required(true),
        )
        .arg(
            Arg::with_name("etag")
                .short("e")
                .long("etag")
                .value_name("TAG")
                .help("The s3 object e-tag")
                .takes_value(true)
                .required(true),
        )
        .get_matches();

    let (path, etag) = match (matches.value_of("filepath"), matches.value_of("etag")) {
        (Some(p), Some(e)) => (p, e),
        _ => {
            eprintln!("Error: Missing parameters");
            std::process::exit(0);
        }
    };
    let mut f = match File::open(path) {
        Ok(file) => file,
        Err(err) => {
            eprintln!("{}", err);
            std::process::exit(0);
        }
    };

    let mut buffer = Vec::new();
    f.read_to_end(&mut buffer).unwrap();
    if buffer.len() == 0 {
        eprintln!("Error: File is empty");
        std::process::exit(0);
    }
    if etag.len() < 32 {
        etag_error()
    }

    let result = guess_etag_parallel(buffer, etag);
    println!("{}", result);
}

fn etag_error() -> ! {
    eprintln!("Error: Etag is not valid");
    std::process::exit(0);
}

fn success_exit_early() -> ! {
    println!("{}", true);
    std::process::exit(0);
}

fn compute_simple(buffer: Vec<u8>, etag: &str, chunks: u32) -> bool {
    let calc_etag = md5::compute(buffer);
    match chunks {
        1 => format!("{:x}-{}", calc_etag, chunks) == etag,
        _ => format!("{:x}", calc_etag) == etag,
    }
}

fn compute_concat(size: (usize, usize), chunks: u32, buffer: Vec<u8>, etag: &str) -> bool {
    match (size.0..size.1).rev().par_bridge().find_any(|size| {
        let final_md5: Vec<u8> = buffer
            .par_chunks(size * 1024 * 1024)
            .into_par_iter()
            .flat_map(|b| md5::compute(b).to_vec())
            .collect();
        let calc_etag = md5::compute(final_md5);
        if format!("{:x}-{}", calc_etag, chunks) == etag {
            success_exit_early();
        }
        false
    }) {
        Some(_) => true,
        None => false,
    }
}

fn guess_etag_parallel(buffer: Vec<u8>, etag: &str) -> bool {
    let mut etag_parts: Vec<&str> = etag.split("-").collect();
    match etag_parts.len() {
        1 => compute_simple(buffer, etag, 1),
        2 => {
            let chunks: u32 = match etag_parts.pop().unwrap().parse() {
                Ok(c) if c > 1 => c,
                _ => etag_error(),
            };
            let file_size = buffer.len() / 1024 / 1024;
            let min_sz = file_size / chunks as usize;
            let max_sz = file_size / (chunks as usize - 1);
            if min_sz == max_sz {
                compute_simple(buffer, etag, chunks)
            } else {
                compute_concat((min_sz, max_sz), chunks, buffer, etag)
            }
        }
        _ => etag_error(),
    }
}
