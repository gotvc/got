package e2etest

import (
	"fmt"
	"testing"

	"github.com/gotvc/got/src/gotrepo"
	"github.com/gotvc/got/src/gottests"
	"github.com/gotvc/got/src/gotwc"
	"github.com/gotvc/got/src/internal/testutil"
	"github.com/stretchr/testify/assert"
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
	site.AssertNotExist("b.txt")
	site.Put("")
	site.Commit(gotwc.CommitParams{})
	// now go back to the original branch
	site.Checkout("master")
	for k, v := range m1 {
		site.AssertFileString(k, v)
	}
}

func TestExport(t *testing.T) {
	t.Parallel()
	type testCase struct {
		// InSnap is the data which will be the Snapshot to be exported.
		InSnap map[string]string
		// InFS is the data which will be in the filesystem at the time Export is called.
		InFS map[string]string
		// Yes is nil if the export should be allowed.
		// If the export is not allowed, then an error is expected, and InFS
		// should match the filesystem after the Export call fails.
		Err error
	}
	tcs := []testCase{
		{

			InSnap: map[string]string{
				"a.txt": "snapshotted",
				"b.txt": "also snapshotted",
			},
			InFS: map[string]string{}, // clear all
		},
		{
			InSnap: map[string]string{
				"a.txt": "snapshotted",
				"b.txt": "also snapshotted",
			},
			InFS: map[string]string{
				"a.txt": "snapshotted",
				// remove b
			},
		},
		{
			InSnap: map[string]string{
				"a.txt": "snapshotted",
			},
			InFS: map[string]string{
				"a.txt": "dirty",
			},
			Err: gotwc.ErrWouldClobber{Path: "a.txt"},
		},
	}
	for i, tc := range tcs {
		name := fmt.Sprintf("%d", i)
		t.Run(name, func(t *testing.T) {
			site := gottests.NewSite(t)
			site.CreateMark(gotrepo.FQM{Name: "master"})
			site.WriteFSMap(tc.InSnap)
			for p := range tc.InSnap {
				site.Add(p)
			}
			site.Commit(gotwc.CommitParams{})

			for p := range tc.InSnap {
				if _, exists := tc.InFS[p]; !exists {
					site.DeleteFile(p)
				}
			}
			for p, val := range tc.InFS {
				site.WriteString(p, val)
			}

			ctx := testutil.Context(t)
			err := site.WC.Export(ctx)
			if tc.Err == nil {
				assert.NoError(t, err)
				site.AssertFSEquals(tc.InSnap)
			} else {
				assert.ErrorIs(t, err, tc.Err)
				site.AssertFSEquals(tc.InFS)
			}
		})
	}
}
