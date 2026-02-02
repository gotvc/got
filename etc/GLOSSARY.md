# Glossary

## Unknown files
Files that do not have entries in the working copy database, or whose database entries are stale. That is all they are. Their contents are unknown until an import is performed, at which point they will have a DB entry and become known.  This is an orthogonal concept to tracked paths.

## Tracked paths
Paths that Got is interested in. The working copy has prefixes in the configuration file which define the bounds for which files are tracked.
- `.got/` paths can never be tracked and are ignored at the lowest possible level in the working copy. A `.got` file can never make it into the database because it is removed from the `posixfs.FS` abstraction before the Importer can look at them.
- Tracked prefixes are how Got allows for partial checkouts.  Got only reads and writes to paths which it is currently tracking.

## Dirty paths
Tracked paths with unstaged changes. If the path is not tracked, then it is not considered dirty.  Unkown files are considered dirty, even though they are only possibly dirty. In Git terms, both “changed” and “untracked” map to Got’s “dirty” (within tracked prefixes); Got does not distinguish these as separate categories.
