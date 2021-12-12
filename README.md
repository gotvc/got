# Got
Like Git, but with an 'o'

Got is version control, with a model similar to Git's, extending ideas from [WebFS](https://github.com/brendoncarroll/webfs).

Got uses [INET256](https://github.com/inet256/inet256) to easily and securely connect repositories.

## Features
- Snapshot based version control.
- Good support for large files.
- End-to-End Encryption.
- Multi-Backend, using [Cells](./pkg/cells) and [Stores](./pkg/stores)

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
If that isn't on your path, just copy the executable from there to whereever you want.

```shell
$ make install
```

## More
Read more about the CLI commands in [docs/cli.md](./docs/cli.md).

Read more about the configuration objects in [docs/config.md](./docs/config.md).

Take a look at [ARCHITECTURE.md](./ARCHITECTURE.md) for more details on how it all works.
