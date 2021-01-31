using Go = import "/go.capnp";
@0xbf31eccc5703e3c8;
$Go.package("gkvproto");
$Go.import("github.com/brendoncarroll/got/pkg/gotkv/gkvproto");

struct Ref {
    cid @0: Data;
    dek @1: Data;
}

struct Node {
    union {
        leaf @0: Leaf;
        tree @1: Tree;
    }
}

struct Entry {
    key @0: Data;
    value @1: Data;
}

struct Leaf {
    entries @0: List(Entry);
}

struct ChildRef {
    prefix @0: Data;
    ref @1: Ref;
}

struct Tree {
    entries @0: List(Entry);
    children @1: List(ChildRef);
}
