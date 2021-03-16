# Got
Like git, but with an 'o'

Got is version control, like git, with ideas from [WebFS](https://github.com/brendoncarroll/webfs).

## Model
Got builds up merkle data structures and stores their root hash in compare-and-swap cells.
Branching and syncing are done by creating CAS cells and writing or reading from them.

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

    cell-specs/
        cell-name
        {
            local: {}, // look in db for data
            secretbox: {
                // recursive
            },
            http: {
                url: "https://somewhere.com/cells/1234/,
                headers: {}
            },
            peer: "<peer_id>",
        }
        cell-name2
        ...

    local.db
```

Realms are layered, with the cell-specs config directory being the highest priority Realm.
