# Got
Like Git, but with an 'o'

Got is version control, with a model similar to Git's.
A solution to the problems that come from using Git to store all of your data.
Got uses an improved data structure that better handles large files and directories, and encrypts all the data that you give it.

## Quick Links
- [Docs](./doc/0_Got.md)
- [CLI Reference](./doc/3.0_CLI.md)
- [GotFS](./src/gotfs/README.md)
- [GotKV](./src/gotkv/README.md)

## Getting Started

### Installation
Either download a prebuilt binary or build one from source.

Installs to `/usr/bin/got` with just.
```shell
$ just install
```
This will build Got for the current architecture, leaving the binary in `build/out/got`.

### Create a New Repo
Then initialize a repository in the current working directory.
Make sure you `cd` to where you want the repository.
```shell
$ got init
```

That will create a new repo using an in-process blobcache.
All content will be stored in the `.got/blobcache` directory.
> This is the recommended way to try out blobcache.

A repo can also be initially configured to use the system (or any) blobcache instance.
```shell
$ got init --blobcache-client <http endpoint> --volume <volume-oid>
```

> For large repositories, it is recommended to use an out of process blobcache

## Contributing
We use `just` as a command runner.
All of the common development tasks have `just` commands.

To run the tests:
```shell
$ just test
```

To build release binaries
```shell
just build-exec
```
The release binaries for all architectures will be under the `build/out` directory.

## Got in Action
Got importing a 3GB file.
```shell
$ got add large_file.dat
[1.608s]
  [1.564s] large_file.dat data_in=(1.24GB Δ=799.35MB/s)
    [1.564s] worker-0 blobs_in=(182blobs Δ=117.26blobs/s), data_in=(157.09MB Δ=101.21MB/s)
    [1.564s] worker-1 blobs_in=(197blobs Δ=127.43blobs/s), data_in=(158.19MB Δ=102.33MB/s)
    [1.564s] worker-2 blobs_in=(181blobs Δ=116.50blobs/s), data_in=(154.83MB Δ=99.66MB/s)
    [1.564s] worker-3 blobs_in=(172blobs Δ=112.63blobs/s), data_in=(154.01MB Δ=100.85MB/s)
    [1.564s] worker-4 blobs_in=(179blobs Δ=116.82blobs/s), data_in=(157.34MB Δ=102.68MB/s)
    [1.564s] worker-5 blobs_in=(177blobs Δ=115.36blobs/s), data_in=(156.12MB Δ=101.75MB/s)
    [1.564s] worker-6 blobs_in=(199blobs Δ=130.09blobs/s), data_in=(155.20MB Δ=101.46MB/s)
    [1.564s] worker-7 blobs_in=(161blobs Δ=104.50blobs/s), data_in=(150.69MB Δ=97.81MB/s)
```

## More
Read more about configuration in [doc/Config.md](./doc/Config.md).

Support and development discussion happen in the INET256 discord.
[<img src="https://discord.com/assets/cb48d2a8d4991281d7a6a95d2f58195e.svg" width="80">](https://discord.gg/TWy6aVWJ7f)
