package main

import (
	"os"

	"github.com/gotvc/got/src/gotcmd"
)

func main() {
	if err := gotcmd.Execute(); err != nil {
		os.Exit(1)
	}
}
