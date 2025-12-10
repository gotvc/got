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
A group contains a set of leaves, and a set of other groups.

Each Group has it's own KEM key.
All members of the group have access to an encrypted version of the KEM private key.
Everyone can see the KEM public key in the record for the Group.

## Rules
The namespace has a set of rules, which are used to grant access to regions of the namespace.
Each rule has 3 parts: a subject, a verb, and an ObjectSet.
Subject refers to a Group by name.
A Group cannot be deleted unless all rules referring to it are also deleted.

Verb is the action being performed, viewing or editing the branch contents or metadata.

ObjectSet is a type plus a regular exprssion.
The Type can either be "group" or "branch".

## Branches
The namespace for branches is regulated by the rules
