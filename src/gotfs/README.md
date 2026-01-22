# GotFS

GotFS is a filesystem implemented on top of a key-value store.
The key value store is GotKV, which is an immutable key-value store, sitting on top of a content-addressed data store.
Every operation on a GotKV store returns a new reference to a new store with the change.
GotFS inherits this property.
All operations return a reference to a new filesystem.
A GotFS filesystem is just a GotKV store with a specific key structure used to represent files and directories.

## Objects
### Ref
A reference to data in the content-addressed store.
This type is provided by GotKV.

### Extent
A part of a file.  It includes a Ref, an offset, and a length.  It is basically an instruction to:

1. Get the referenced data from content-addressed storage
2. Only include the data from `offset` to `offset+length`.

This enables packing small files into the same blob.

### Info
Information about a file or directory.
Most importantly the permissions and type of file.

## Key Layout 
All objects are represented by an Info entry at a specific key, and content stored under keys
prefixed with the metadata key.

File data is stored in a content-addressed store, and references to the data are stored in GotKV.

### Example: 1 File
For example: The file "test.txt" with 10B of data in it would produce the following key value pairs.
```
/<NULL>< 64 bit: 0  >            -> Info (dir)
/test.txt/<NULL>< 64 bit: 0  >   -> Info (file)
/test.txt/<NULL>< 64 bit: 10 >   -> Extent
```

### Example: 2 File + 1 Directory
A directory is stored as a metadata object.
```
/<NULL>< 64 bit: 0  >                     -> Info (dir)
/mydir/<NULL>< 64 bit: 0 >                -> Info (dir)
/mydir/myfile.txt<NULL>< 64 bit: 0     >  -> Info (file)
/mydir/myfile.txt<NULL>< 64 bit offset >  -> Part
```

### Example 3: File at the Root
It is possible for a file to be at the root
```
/<NULL>< 64 bit: 0      >       -> Info (file)
/<NULL>< 64 bits extent >       -> Extent
/<NULL>< next offset    >       -> Extent
```

All Info keys end in a trailing `/`, including regular files.
Keys for Info objects contain no NULL characters.
Keys for extents contain exactly 1 NULL character 9 bytes from the end of key, separating the path from the offset.
Extent keys are always prefixed by the Info key for the file they are part of.

## Reading A File
To read from a file in GotFS you first lookup the Info entry for the path of the file.
If there is not an entry at the path or the entry is not for a regular file, then return an error.

Then convert the offset to read from to a key.
Seek to the first entry after that key.
The extent referenced by that entry will end at the offset in the key, and will contain data overlapping the target range.
