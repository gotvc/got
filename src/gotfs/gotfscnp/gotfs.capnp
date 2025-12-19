@0xc68fdce9f9749f41;
using Go = import "/go.capnp";

$Go.package("gotfscnp");
$Go.import("github.com/gotvc/got/src/gotfs/gotfscnp");

struct Info {
    mode @0 :UInt32;
    attrs @1 :List(Attr);
}

struct Attr {
    key @0 :Text;
    value @1 :Data;
}
