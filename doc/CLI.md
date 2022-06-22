# CLI

## Setup

### `got init`
Initializes a Got repo in the current working directory.

### `got clone <dst> <url>`
Initializes a new Got repo in the directory dst with branches under `origin/` mapping to the space at the URL.

## General

### `got status`
Prints information about the active branch, staging area, and untracked paths.

### `got commit [-m <message>]`
Creates a new snapshot by applying any changes in tracked paths to the current branch.
The current branch's head is updated to the new snapshot.

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

## Staging
These commands control what content will be committed.

### `got add <path>`
Add the files at or below path to the staging area.

### `got put <path>`
Add the file or directory to the staging area.
`put` on a file is that same as `add`.
`put` on a directory will also delete any files not in the directory.

### `got rm <path>`
Mark the file for deletion in the staging area.
 
### `got discard <path>`
Discard any staged operations for this path.

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

## IAM

### `got iam`
Prints the access control policy from `.got/policy`.

## Misc

### `got slurp <path>`
Slurp creates a gotfs filesystem from the file at path and writes the root, PEM encoded, to stdout.
Slurp can reference paths outside of the repo.
It does not write to any branches.
It exists primarily to give an idea of how fast Got can import a file or directory.

### `got clobber <path>`
Overwrites the data in the working tree at path with whatever is in the active branch

### `got scrub`
Runs validation checks on the snapshots in the current history and their filesystems.
