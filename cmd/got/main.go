package main

import (
	"log"

	"github.com/brendoncarroll/got/pkg/gotcmd"
)

func main() {
	if err := gotcmd.Execute(); err != nil {
		log.Fatal(err)
	}
}
