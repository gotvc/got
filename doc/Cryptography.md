# Cryptography

This document provides an overview of the cryptography in Got.

## Glossary
- `salt` is used throughout to indicate material used as input to a hash or XOF in order to diverge the output.
- `seed` is used throughout to indicate some value which will affect the shape of some data structure, but is not used to derive keys, or perform encryption. Examples include content defined chunking, and split points in probabilistic data structures.

## Primitives
- All encryption in Got is done with `ChaCha20`.
- Authenticated encryption is done using the AEAD `XChaCha20Poly1305`.
- Hashing is done with `BLAKE2b_256`
- Key derivation is done with keyed `BLAKE2b_XOF`
- `SipHash` is used to determine the structure of data structures. (e.g. `gotkv/ptree`).

## Key Derivation
Got derives all keys using the `BLAKE2b` XOF.
The XOF is created with a 32 byte key, and additional material is written into the XOF.
Then the new key is read from the XOF.

## Content-Addressed Data Encryption
Content-Addressed data is encrypted using convergent encryption--meaning the key is derived from the data to be encrypted.
The "content identifier" (CID) refers to the hash of the data.
In Got this is always the `BLAKE2b_256` of the data.

The branch salt is used to derive data encrytion keys (DEK) when writing blobs to the branch.

```
plain_text_hash = BLAKE2b_256(plain_text)
dek = DeriveKey(salt, plain_text_hash)
```

The blob data would then be encrypted using the DEK.
The ciphertext is hashed to produce the CID.
Both the DEK and CID are stored as a reference to the data.

## Tour
- `branches/crypto.go`: contains the encrypted `branches.Space` implementation
- `cells/cells.go`: AEAD for encrypted cells.
- `gdat/crypto.go`: symmetric encryption for all content addressed data.
