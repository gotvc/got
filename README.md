# Got
Like git, but with an 'o'

Got is version control, like git, with ideas from [WebFS](https://github.com/brendoncarroll/webfs).

Take a look at [ARCHITECTURE.md](./ARCHITECTURE.md) for more details

## Features
- Version control similar to Git
- Good support for large files
- Data is end to end encrypted.

## Config
Config is stored under the `.got` directory
```
.got/
    config
    {
        "realms": [
            {},
        ]
    }

    volume_specs/
        volume-name1
        {
            cell: {
                local: {}, // look in db for data
                secret_box: {
                    // recursive
                },
                signed: {
                    // recursive
                }
                http: {
                    url: "https://somewhere.com/cells/1234/,
                    headers: {}
                },
                peer: {
                    id: "<peer_id>",
                    name: "<cell name on remote>",
                }
            },
            store: {
                local: {id: 1234} // local store
            },
        }
        volume-name2
        ...

    local.db
```
