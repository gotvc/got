# GotNS

GotNS is a namespace for branches.
It manages all of the public key cryptography needed to enforce permissions for reading and writing from branches.

The branch Volumes are encrypted with AEADs, and the secret keys are stored encrypted in GotNS.

All actions in GotNS must be signed by a single key.

## Identity
Identities are a set of public keys used for signing.
Identities are either Leaves, or Groups.

### Leaves
Leaves are uniquely identified by an INET256 address, derived by hashing the public key.
A KEM key pair is generated and then the public key is signed with the signing key.

### Groups
A group is uniquely defined by a user specified name.
Names are unique within the namespace.

The simplest identity is a single signing key, which u

## Rules
The branch namespace
