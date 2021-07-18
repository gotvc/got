package main

import (
	"log"

	"github.com/gotvc/got/pkg/gotcmd"
)

func main() {
	if err := gotcmd.Execute(); err != nil {
		log.Fatal(err)
	}
}
