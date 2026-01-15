
CREATE TABLE staging_areas (
    rowid INTEGER PRIMARY KEY AUTOINCREMENT,
    param_hash BLOB NOT NULL UNIQUE
), STRICT;

CREATE TABLE staging_ops (
    area_id INTEGER NOT NULL REFERENCES staging_areas(rowid),
    p TEXT NOT NULL,
    data BLOB NOT NULL,
    PRIMARY KEY (area_id, p)
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

CREATE TABLE extents (
    path TEXT NOT NULL,
    start_at INTEGER NOT NULL,

    end_at INTEGER NOT NULL,
    ref BLOB NOT NULL,
    PRIMARY KEY (path, start_at)
), WITHOUT ROWID, STRICT;
