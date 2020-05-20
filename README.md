# s3etag

An executable to know if an s3 file was downloaded correctly

## How to use

```
s3etag -h

s3etag 1.0
It compares a file checksum to an s3 e-tag

USAGE:
    s3etag --etag <TAG> --filepath <FILE>

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

OPTIONS:
    -e, --etag <TAG>         The s3 object e-tag
    -f, --filepath <FILE>    The path of the file you want to check
```
