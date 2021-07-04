# Config
Config is stored under the `.got` directory
```
.got/
    config
    {
        "realms": [
            {},
        ]
    }

    branches/
        branch-name1
        {
            volume: {
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
                vc_store: {
                    local: {id: 1234} // local store
                },
                fs_store: {
                    local: {id: 2345} // local store
                },
                raw_store: {
                    local: {id: 3456} // local store
                }
            }
        }
        branch-name2
        ...

    local.db
```
