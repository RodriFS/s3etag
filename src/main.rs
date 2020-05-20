use clap::{App, Arg};
use md5;
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

    let path = matches.value_of("filepath").unwrap();
    let etag = matches.value_of("etag").unwrap();

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
        eprintln!("Error: Etag is not valid");
        std::process::exit(0);
    }

    guess_etag(buffer, etag);
}

fn guess_etag(buffer: Vec<u8>, etag: &str) {
    let mut etag_parts: Vec<&str> = etag.split("-").collect();
    if etag_parts.len() == 2 {
        let chunks: i32 = etag_parts.pop().unwrap().parse().unwrap();
        let file_size = buffer.len() / 1024 / 1024;
        let min_sz = file_size / chunks as usize;
        let max_sz = file_size / (chunks as usize - 1);

        for size in min_sz..max_sz {
            let final_md5: Vec<u8> = buffer
                .chunks(size * 1024 * 1024)
                .flat_map(|b| md5::compute(b).to_vec())
                .collect();
            let calc_etag = md5::compute(final_md5);
            if format!("{:x}-{}", calc_etag, chunks) == etag {
                println!("match found");
                std::process::exit(0);
            }
        }
        eprintln!("Error: File is corrupted");
    } else {
        let calc_etag = md5::compute(buffer);
        if format!("{:x}", calc_etag) == etag {
            println!("match found");
            std::process::exit(0);
        }
        eprintln!("Error: File is corrupted");
    }
}
