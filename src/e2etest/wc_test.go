package e2etest

import (
	"testing"

	"github.com/gotvc/got/src/gotrepo"
	"github.com/gotvc/got/src/gottests"
	"github.com/gotvc/got/src/gotwc"
)

func TestCheckout(t *testing.T) {
	site := gottests.NewSite(t)
	site.CreateMark(gotrepo.FQM{Name: "master"})

	// commit some files
	m1 := map[string]string{
		"a.txt": "file data a",
		"b.txt": "file data b",
		"c.txt": "file data c",
	}
	site.WriteFSMap(m1)
	site.Put("")
	site.Commit(gotwc.CommitParams{})
	// fork
	site.Fork("fork")
	// change a and c, delete b
	m2 := map[string]string{
		"a.txt": "file data a 2",
		"c.txt": "file data c 2",
	}
	site.WriteFSMap(m2)
	site.DeleteFile("b.txt")
	site.Put("")
	site.Commit(gotwc.CommitParams{})
	// now go back to the original branch
	site.Checkout("master")
	for k, v := range m1 {
		site.AssertFileString(k, v)
	}
}
