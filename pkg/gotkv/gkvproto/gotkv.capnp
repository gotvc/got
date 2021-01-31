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
        child @0: Child;
        parent @1: Parent;
    }
}

struct Entry {
    key @0: Data;
    value @1: Data;
}

struct Child {
    entries @0: List(Entry);
}

struct ChildRef {
    commonPrefix @0: Data;
    ref @1: Ref;
}

struct Parent {
    entry @0: Entry;
    children @1: List(ChildRef);
}
