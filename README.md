# Got
Like Git, but with an 'o'

Got is version control, with a model similar to Git's.
It provides end-to-end encryption and uses an improved data structure with good support for large files and directories.

Got uses [INET256](https://github.com/inet256/inet256) to easily and securely connect repositories.

[CLI Docs](./doc/CLI.md)

[ARCHITECTURE.md](./ARCHITECTURE.md)

[Cryptography Overview](./doc/Cryptography.md)

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
- [x] Share repositories over INET256 using `serve` command.
- [x] Branch level access control using ACL defined in a `.got/policy` file.
- [x] Efficiently pack many small files into fewer blobs.
- [ ] Checkout the head of a branch to the working directory.
- [ ] Merge 2 branches.

## Useful Components
- [GotKV](./pkg/gotkv/README.md)
- [GotFS](./pkg/gotfs/README.md)

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
$ make test
```

Installs to `$GOPATH/bin` with make.
If that isn't on your path, just copy the executable from there to wherever you want.

```shell
$ make install
```

To build release binaries
```shell
TAG=v0.0.x make build
```
Where `TAG` is an environment variable which should be set to the release version, or the Git hash of the source used for the build.
The release binaries will be under the `build` directory.

## More
Read more about the configuration objects in [docs/config.md](./docs/config.md).

