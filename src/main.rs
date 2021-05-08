use clap::{App, Arg};
use rayon::prelude::*;
use std::fs::File;
use std::io::Read;

fn main() {
    let matches = App::new("s3etag")
        .version("1.1")
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
        .arg(
            Arg::with_name("chunk_size")
                .short("c")
                .long("chunksize")
                .value_name("SIZE IN MB")
                .help("The s3 object chunk size, if known")
                .takes_value(true)
                .required(false),
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
    if buffer.is_empty() {
        eprintln!("Error: File is empty");
        std::process::exit(0);
    }
    if etag.len() < 32 {
        etag_error()
    }

    let result = guess_etag_parallel(buffer, etag, matches.value_of("chunk_size"));
    println!("{}", result);
}

fn etag_error() -> ! {
    eprintln!("Error: Etag is not valid");
    std::process::exit(0);
}

fn success_exit_early() -> ! {
    println!("{:?}", true);
    std::process::exit(0);
}

fn compute_simple(buffer: Vec<u8>, etag: &str, chunks: u32) -> bool {
    let calc_etag = md5::compute(buffer);
    match chunks {
        1 => format!("{:x}-{}", calc_etag, chunks) == etag,
        _ => format!("{:x}", calc_etag) == etag,
    }
}

#[derive(Debug)]
enum Size {
    Fixed(usize),
    Variable((usize, usize)),
}

fn compute(size: usize, chunks: usize, buffer: &[u8], etag: &str) {
    let final_md5: Vec<u8> = buffer
        .par_chunks(size * 1024 * 1024)
        .into_par_iter()
        .flat_map(|b| md5::compute(b).to_vec())
        .collect();
    let calc_etag = md5::compute(final_md5);
    if format!("{:x}-{}", calc_etag, chunks) == etag {
        success_exit_early();
    }
}

fn compute_concat(size: Size, chunks: usize, buffer: Vec<u8>, etag: &str) -> bool {
    match size {
        Size::Fixed(c) => {
            compute(c, chunks, &buffer, etag);
            false
        }
        Size::Variable(size) => {
            (size.0..size.1 + 1).into_par_iter().rev().for_each(|size| {
                compute(size, chunks, &buffer, etag);
            });
            false
        }
    }
}

fn guess_etag_parallel(buffer: Vec<u8>, etag: &str, chunk_size: Option<&str>) -> bool {
    let mut etag_parts: Vec<&str> = etag.split('-').collect();
    match etag_parts.len() {
        1 => compute_simple(buffer, etag, 1),
        2 => {
            let chunks: usize = match etag_parts.pop().unwrap().parse() {
                Ok(c) if c > 1 => c,
                _ => etag_error(),
            };
            if let Some(c) = chunk_size {
                let chunk_size: usize = c.parse().expect("Invalid chunk size");
                return compute_concat(Size::Fixed(chunk_size), chunks, buffer, etag);
            }

            let file_size = buffer.len() / 1024 / 1024;
            let min_sz = file_size / chunks;
            let max_sz = file_size / (chunks - 1);
            if min_sz == max_sz {
                compute_concat(Size::Fixed(min_sz), chunks, buffer, etag)
            } else {
                compute_concat(Size::Variable((min_sz, max_sz)), chunks, buffer, etag)
            }
        }
        _ => etag_error(),
    }
}
