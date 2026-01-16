
CREATE TABLE staging_ops (
    p TEXT NOT NULL,
    data BLOB NOT NULL,
    PRIMARY KEY (p)
), WITHOUT ROWID, STRICT;

CREATE TABLE dirstate (
    path TEXT NOT NULL,

    mode INTEGER NOT NULL,
    modtime BLOB NOT NULL,

    PRIMARY KEY(path)
), WITHOUT ROWID, STRICT;

CREATE TABLE fsroots (
    param_hash BLOB NOT NULL REFERENCES staging_areas(param_hash),
    path TEXT NOT NULL REFERENCES dirstate(path),

    fsroot BLOB NOT NULL,

    PRIMARY KEY(param_hash, path)
), WITHOUT ROWID, STRICT;
