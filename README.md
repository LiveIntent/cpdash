# Tool for dumping objects from s3 to stdout

`cpdash` is a small cli tool for printing the content of objects on s3 to stdout. Objects will be gunzipped before printing, if that fails the raw content will be printed.

**Note**: Remember that aws will charge you by the byte for data moved from s3 to the internet.

## Installation

If you have Go installed, installing `cpdash` is as easy as

```sh
go install ./cmd/cpdash
```

See `go help install` for more info.

## Usage

The command

```sh
cpdash "s3://bucket/prefix**"
```

is intended to provide functionality similar to what one would expect from the call

```sh
aws s3 cp --recursive s3://bucket/prefix -
```

Currently, aws-cli does not support streaming in combination with `--recursive`. A poor substitute is given by

```sh
aws s3 ls --recursive s3://bucket/prefix | awk '{print($4)}' | xargs -n 1 -P 32 -I {} sh -c "aws s3 cp s3://bucket/{} -"
```

That is, download all objects prefixed by `prefix` in the bucket `bucket` in parallel, printing the content to stdout. The main additional features of cpdash are:

- Content of different objects will not be interleaved

- Objects that are gzip or zstd compressed will have their content decompressed before printing

- More general globbing than just `**` at the end

## bugs

Group globs will not work if the groups contain path separators `/`, i.e. commands like

```sh
cpdash "s3://bucket/prefix/{pattern1/pattern2,pattern3/pattern4}/*"
```

will silently not match any paths.

## Flags

```sh
cpdash -h
```

## License

Licensed under the Apache License, Version 2, see LICENSE for more information
