# Got Architecture

## Data Infrastructure
There is a lot that goes into just storing data, and moving it around, independent of the format.

The concepts convered in this section are universally applicable to the storage and transmission of data structures in a distributed system.

### Cells
Compare and swap cell. Anything that supports reading, and compare-and-swap can be used as a cell.
The interface is in `./pkg/cells`

### Stores
Content-addressed store.
You give it data, it gives you a hash.
You give it a hash, it gives you the data.
Data can be deleted by the hash.
The set of hashes in the store can be listed.

### Volume
A volume is a (Cell, Store) pair.
The exact definition can be found in `pkg/branches`
The cell stores the root of some data structure.
The store stores the content-addressed blobs that make up the data structure.

There are no assumptions made about the contents of either the cell or the store.
This allows them to be encrypted, and operations on volumes to be truly protocol agnostic.
It is still possible to define broadly useful operations on volumes even with this limitation.

The `Sync` operation is defined on two volumes.
The sync operation first ensures the destination Store is a superset of the source Store.
Then it performs a CAS on the destination cell informed by the contents of the source cell.
The term "informed" is used here, intentionally vaugue, to indicate that whatever is calling the Sync operation
should make a decision about the final value in the destination cell, it may not be appropriate to complete the CAS.
The particular logic implemented by the caller is non-essential to the Sync operation.

The `Cleanup` operation is defined on a single volume
The cleanup operation looks at the contents of the cell, and then deletes items from the Store.
While a cleanup operation is happening, all attempts to Sync to the volume must fail.

Sync and Cleanup can be run concurrently with themselves, but not each other.
This essentially puts the volume in one of 2 modes: Expand mode and Contract mode.
How this is implemented is not really important either; you can use locks, you can use MVCC, whatever.

## Branches
Branches are name volumes, and associated metadata like creation time.  Since they contain volumes, they function as a holder of a Got data structure.

### Spaces
A Space is a namespace for addressing a set of branches with the same implementation.
The interface is defined in `pkg/branches`.

One of Got's core functionalities is to be a composite Space.
You stitch together spaces to create a composite namespace of branches, then you perform operations involving the volumes by name.

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

### GotFS
GotFS is a filesystem, supporting directories and regular files.
It is just a GotKV instance with a special schema.

More information in `docs/gotfs.md`

### Snapshots
A Snapshot or Snap is a GotFS instance, as well as a history of snapshots that came before it.
It contains a Ref to a GotFS root.
It may contain a pointer to a parent, so that a delta can be computed.
If there is no parent, then it is the first version of the filesystem.

A serialized Snapshot is what Got stores in a Volume's cell.
And the set of blobs reachable from the Commit via the GotFS datastructure is what is stored in the Volume's store.
