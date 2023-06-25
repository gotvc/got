# Identity and Access Management (IAM)

When a Got Space is hosted remotely, it is useful to be able to remotely configure
who can do what.
Instead of encouraging a proliferation of out-of-band mechanisms for configuring IAM, Got suggests
a simple IAM model and configuration mechanism: The *Got Host Configuration Protocol*.
This is a general strategy for configuring the host of a Space, but it's primary use is IAM.

The default on-disk repository supports hosting a Space configured by this mechanism.

IAM is manipulated, both on the client and server side, using the `got iam` commands.

## Got Host Configuration
If the host of a Got Space supports the configuration protocol, there will be a branch `__host__`,
which cannot be deleted, or created.

e.g.
```
master
origin/__host__
origin/master
```

That branch has a zero salt, and must contain host readable plaintext data.
Got supports E2EE of branches, but this branch cannot be encrypted, as the host would not be able to understand it.

That branch's filesystem will contain a `POLICY` file with rules about who can look at and touch the branches.
The filesystem will also contain an `IDENTITIES` directory.
Each file in that directory is an Identity named by it's path.

## Identity
Identities in Got are just sets of INET256 addresses that have been given a name.
INET256 addresses are the atomic elements of Identity sets.

Set elements can be specified in 3 ways.
- `PEER` A single INET256 address, specified as a literal.
- `ANYONE` the set containing all addresses.
- `NAMED` a reference to another identity by name.  An identity referring to another identity is a superset of that identity.

## Authorization
The `POLICY` file contains rules, 1 per line.

Rules are specified as
1. a directive (ALLOW|DENY).  If this rule matches a request it will be denied, unless overridden.
2. a subject (PEER|@NAME|ANYONE). Specifies an identity that this rule matches.
2. an action (LOOK|TOUCH).  Look means you can read, but not modify.  Touch means you can read and modify.
3. A set of branches specified as a regular expression.

For each action taken on the space, all of the rules are evaluated.
Initially permission is not allowed.
When a rule matches the subject, action, and object of the operation, then permission is set to allow or deny.
The permission can be changed when the next rule is evaluated.
When all the rules have been evaluated, the resulting permission determines whether the action can be performed.
Since the initial state is deny, if there is not a rule granting the operation, then it is blocked by default.

### Admins
The authorization policy applies to all the branches including the `__host__` branch.
This means that an administrator (with the ability to manage IAM), is just someone with write access to the `__host__` branch.

You cannot become locked out of a local got repository.
IAM is not enforced by the got command on itself, while it is manipulating the repository.
