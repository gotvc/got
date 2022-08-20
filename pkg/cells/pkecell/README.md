# Public Key Encryption Cell

Package `pkecell` implements a cell with public key encryption in terms of another cell and a Key Encapsulation Mechanism (KEM).

## Format
The cell is divided into distinct messages using length prefixes.
```
| varint | <-- variable length section 1 --> | ... | varint | <-- variable length section N --> | 
```

If a varint would lead to out of bounds access or the whole cell contents is not referenced by the sub sections then it is a parsing error and no attempt will be made to interpret the contents.

There are 2 sections to the cell contents.

The first message contains an array of ciphertexts to the long-term key of each receiving party.  Each ciphertext is 2 parts.
- A KEM ciphertext used to establish a shared secret
- A AEAD ciphertext using the KEM's shared secret
The corresponding AEAD plaintext contains 2 pieces of information, the encryption key used for the last part of the message, the hash of the signing key and a signature of the 2nd part of the message.

The last message contains a AEAD sealed ciphertext of the cell contents, using the randomly derived secret.

In essence a secure tunnel is created using a KEM and the shared secret derived from that is signed using a long term signing key.
That signature and a payload (the main DEK) are sent inside the secure tunnel.
