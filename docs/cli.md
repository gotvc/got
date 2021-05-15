# CLI

## General

### `got status`
This prints information about the active volume and the contents of staging.

### `got branch <name>`
Branch switches to the volume at name, or creates volume with the provided name and initializes it with the content of the current volume.

### `got commit [-m <message>]`
Produces a delta by looking at the tracked paths and applies that delta to the contents of the active volume.
This produces a new commit.
The new commit is written to the active volume, replacing what was there.

### `got ls <path>`
Lists the children of path in the filesystem contained in the current volume.

### `got cat <path>`
Writes the contents of the file at path, from the filesystem contained in the current volume, to stdout.

## Tracking
These commands manage what content will be committed.

### `got track <path>`
Start tracking the path. The path will be considered during the next commit.
If the path does not exist, then it will be deleted.

### `got untrack <path>`
Stop tracking the path, if it is tracked.

### `got clear`
Untracks everything.

## Volumes

### `got ls-vol`
Lists the available volumes

### `got new-vol <name> `
Creates a new volume with configuration written to stdin.
You can also create a volume by manually writing the configuration to the `.got/volume_specs` directory.

### `got rm <name>`
Deletes the volume with name if it exists

### `got sync <src_vol> <dst_vol>`
Sync the contents of src_vol to dst_vol.

## Misc

### `got slurp <path>`
Slurp creates a gotfs filesystem from the file at path and writes the root, PEM encoded, to stdout.
Slurp can reference paths outside of the repo.
It does not write to any volumes.

### `got clobber <path>`
This overwrites the data in the working tree at path with whatever is in the active volume

### `got check`
Runs validation checks on the snapshots in the current history and their filesystems.
