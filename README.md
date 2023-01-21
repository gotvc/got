# Got
Like Git, but with an 'o'

Got is version control, with a model similar to Git's.
It provides end-to-end encryption and uses an improved data structure with good support for large files and directories.

## Quick Links
- [ARCHITECTURE.md](./ARCHITECTURE.md)
- [CLI Docs](./doc/CLI.md)
- [Cryptography Overview](./doc/Cryptography.md)
- [GotFS](./pkg/gotfs/README.md)
- [GotKV](./pkg/gotkv/README.md)

## Features/Roadmap
- [x] Content-Defined chunking of large files into blobs with a maximum size.
- [x] E2E encryption. Branch names, directory names, file names, and file sizes can all be hidden from remote repositories.
- [x] Efficiently add/remove large files and directories to/from existing filesystems.
- [x] Stage changes with `add`, `rm`, `put`, and `discard` commands.
- [x] Inspect branch state with `cat` and `ls`.
- [x] Print and change the active branch with `active` command.
- [x] Commit changes, with `commit`.
- [x] Create, delete, and list branches with `branch` commands.
- [x] Copy one branch state to another with `sync`.
- [x] Share repositories over [INET256](https://github.com/inet256/inet256) using `serve` command.
- [x] Share repositories using QUIC with the `serve-quic` command.
- [x] Branch level access control using ACL defined in a `.got/policy` file.
- [x] Efficiently pack many small files into fewer blobs.
- [ ] Checkout the head of a branch to the working directory.
- [ ] Merge 2 branches.

## Getting Started
Either download a prebuilt binary or build one from source.

Then initialize a repository in the current working directory.
Make sure you `cd` to where you want the repository.
```shell
$ got init
```

## Contributing
To run the tests:
```shell
$ just test
```

Installs to `$GOPATH/bin` with just.
If that isn't on your path, just copy the executable from there to wherever you want.

```shell
$ just install
```

To build release binaries
```shell
TAG=v0.0.x just build
```
Where `TAG` is an environment variable which should be set to the release version, or the Git hash of the source used for the build.
The release binaries will be under the `out` directory.

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
Read more about the configuration objects in [doc/Config.md](./doc/Config.md).

Support and development discussion happen in the INET256 discord.
[<img src="https://discord.com/assets/cb48d2a8d4991281d7a6a95d2f58195e.svg" width="80">](https://discord.gg/TWy6aVWJ7f)
