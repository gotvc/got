package main

import (
	"github.com/gotvc/got/pkg/gotcmd"
	"github.com/sirupsen/logrus"
)

func main() {
	if err := gotcmd.Execute(); err != nil {
		logrus.Fatal(err)
	}
}
