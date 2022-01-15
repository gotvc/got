# GotKV
GotKV is a copy-on-write, encrypted, deterministic, key value store built on top of content-addressed storage.

## Example: Put/Get
```go
package main

import "github.com/gotvc/got/pkg/gotkv"

func main() {
    // create operator
    avgSize := 1 << 13
    maxSize := 1 << 16
    kvop := gotkv.NewOperator(avgSize, maxSize)

    // create store
    s := cadata.NewMem(cadata.DefaultHash, maxSize)

    // ignoring errors for brevity
    x, _ := op.NewEmpty(ctx, s)
    x, _ = op.Put(ctx, s, *x, []byte("my key"), []byte("my value"))
    value, _ := op.Get(ctx, s, *x, []byte("my key"))
    // value == []byte("my value")
} 
```

## Example: Builder
```go
package main

import "github.com/gotvc/got/pkg/gotkv"

func main() {
    ctx := context.Background()
    // create operator
    avgSize := 1 << 13
    maxSize := 1 << 16
    kvop := gotkv.NewOperator(avgSize, maxSize)

    // create store
    s := cadata.NewMem(cadata.DefaultHash, maxSize)

    // ignoring errors for brevity
    b := kvop.NewBuilder(s)
    for i := 0; i < 10; i++ { 
        b.Put(ctx, []byte("key-" + strconv.Itoa(i)), []byte("my value"))
    }
    root, _ := b.Finish(ctx)
    // the instance rooted at root contains key-0 through key-10
}
```