# CLI


## General

### `got status`
This prints information about the active volume and the contents of staging.

### `got branch <name>`
Branch switches to the volume at name, or creates volume with the provided name and initializes it with the content of the current volume.

### `got commit [-m <message>]`
Applies the contents of staging to the contents of the active volume, producing a new commit.
The new commit is written to the current volume, replacing what was there.

### `got ls <path>`
Lists the children of path in the filesystem contained in the current volume.

### `got cat <path>`
Writes the contents of the file at path, from the filesystem contained in the current volume, to stdout.

## Staging
These commands add and remove files from the staging area.

### `got add <path>`
Marks a file from the working tree to create or replace the corresponding file in staging.

### `got rm <path>`
Marks a file to be removed in staging.

### `got unstage <path>`
Removes a path from staging.  It will no longer be marked for addition or deletion if it was.

### `got clear`
Unstages everything from staging.
This is equivalent to calling `unstage` on every path in staging

**NOTE UNSTABLE: may be added as a flag to unstage**

## Volumes

### `got ls-vol`
Lists the available volumes

### `got new-vol <name> `
Creates a new volume with configuration written to stdin.
You can also create a volume by manually writing the configuration to the `.got/volume_specs` directory.

### `got rm <name>
Deletes the volume with name if it exists

## Misc

### `got slurp <path>`
Slurp creates a gotfs filesystem from the file at path and writes the root, PEM encoded, to stdout.
Slurp can reference paths outside of the repo.
It does not write to any volumes or to staging.

### `got clobber <path>`
This overwrites the data in the working tree at path with whatever is in staging + the active volume.