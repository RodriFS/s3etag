# s3etag

Parallel computation of s3 multipart e-tag

## How to use

```
s3etag -h

s3etag 1.1
It compares a file checksum to an s3 e-tag

USAGE:
    s3etag --etag <TAG> --filepath <FILE>

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

OPTIONS:
    -c, --chunksize <SIZE IN MB> The s3 object chunk size, if known
    -e, --etag <TAG>         The s3 object e-tag
    -f, --filepath <FILE>    The path of the file you want to check
```
