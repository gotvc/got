# Got Architecture

## Volumes
Volumes are a mutable cell for storing data, paried with a content adressable store.
The exact definition can be found in `pkg/branches`
The cell stores the root of some data structure.
The store stores the content-addressed blobs that make up the data structure.

Got actually uses multiple stores per volume, so that data and different levels of metadata can be treated differently.

There are no assumptions made about the contents of either the cell or the store.
This allows them to be encrypted, and operations on volumes to be truly protocol agnostic.
It is still possible to define broadly useful operations on volumes even with this limitation.

## Branches
Branches are named volumes, and associated metadata like creation time.
Since they contain volumes, they function as a holder of a Got data structure.

## Spaces
A Space is a namespace for addressing a set of Branches with the same implementation.
The interface is defined in `pkg/branches`.

One of Got's core functionalities is to be a composite Space.
You stitch together Branch Spaces to create a composite namespace of branches, then you perform operations involving the branches by name.

## Data Formats
There are a few layers that make up Got's data formats.

### Refs
All data is encrypted, and the key for encryption (regardless of how it was derived) is stored with the hash reference to the data.
So Got's Refs are a ciphertext hash, and a secret key, for a total of about 64 bytes per ref.
Anytime you see "Ref", "reference", or "pointer" mentioned below, it is referring to one of these Refs.

### GotKV
GotKV is a key value store built on top of a Store, using Refs.
GotKV is a persistent data structure, any change returns a reference to a new data structure which (likely) overlaps with the original.
There is a practical limit on the length of keys and values, both should be somewhat small, way smaller than the maximum blob size.
GotKV is implemented using a probabilistic B-Tree.

### GotFS
GotFS is a filesystem, supporting directories and regular files.
It is just a GotKV instance with a special schema.

More information in `doc/gotfs.md`

### Snapshots
A Snapshot or Snap is a GotFS instance, as well as a history of snapshots that came before it.
It contains a Ref to a GotFS root.
It may contain a pointer to a parent, so that a delta can be computed.
If there is no parent, then it is the first version of the filesystem.

A serialized Snapshot is what Got stores in a Volume's cell.
And the set of blobs reachable from the Commit via the GotFS data-structure is stored in the Volume's store.
