# CLI

## General

### `got status`
Prints information about the active branch, and tracked paths.

### `got commit [-m <message>]`
Creates a new snapshot by applying any changes in tracked paths to the current branch.
The current branch's head is updated to the new snapshot.
Produces a delta by looking at the tracked paths and applies that delta to the contents of the active volume.
This produces a new commit.
The new commit is written to the active volume, replacing what was there.

### `got fork <name>`
1. Creates a new branch called name, or errors if it exists.
2. Syncs the active branch to the new branch.
3. Sets the new branch as the active branch.

This command is how history splits.  It is analagous to `git checkout -b <name>`

### `got history`
Prints the active branch Snapshot chain starting with the branch head.
Similar to `git log`.

### `got ls <path>`
Lists the children of path in the filesystem contained in the current branch.

### `got cat <path>`
Writes the contents of the file at path, from the filesystem contained in the current branch, to stdout.

## Tracking
These commands manage what content will be committed.

### `got track <path>`
Start tracking the path. The path will be considered during the next commit.
If the path does not exist, then it will be deleted.
A path will be interpretted as both a file and a directory so `foo` will try to add or delete a file `foo`, and all files under a directory `foo`.

### `got untrack <path>`
Stop tracking the path, if it is tracked.
Does not error if the path is not tracked.

### `got clear`
Untracks everything.

## Branches

### `got active [name]`
If `name` is provided, switches to the branch with that name.  If no name is provided, prints the active branch.

### `got branch list`
Lists the branches in the root branch space.

### `got branch create <name>`
Creates a new branch.

### `got branch delete <name>`
Deletes the branch with name if it exists.
Does not error if the branch does not exist.

### `got branch set-head <name>`
Sets the head of the branch at name to a Snapshot parsed from standard input.

### `got branch get-head <name>`
Prints the head of the branch at name to standard output.

### `got sync <src> <dst>`
Sync the contents of branch `<src>` to branch `<dst>`.

## Misc

### `got slurp <path>`
Slurp creates a gotfs filesystem from the file at path and writes the root, PEM encoded, to stdout.
Slurp can reference paths outside of the repo.
It does not write to any branches.
It exists primarily to give an idea of how fast Got can import a file or directory.

### `got clobber <path>`
Overwrites the data in the working tree at path with whatever is in the active volume.

### `got scrub`
Runs validation checks on the snapshots in the current history and their filesystems.
