
CREATE TABLE staging_areas (
    rowid INTEGER PRIMARY KEY AUTOINCREMENT,
    salt BLOB NOT NULL UNIQUE
), STRICT;

CREATE TABLE staging_ops (
    area_id INTEGER NOT NULL REFERENCES staging_areas(rowid),
    p TEXT NOT NULL,
    data BLOB NOT NULL,
    PRIMARY KEY (area_id, p)
), WITHOUT ROWID, STRICT;
