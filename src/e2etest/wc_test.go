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
	t.Parallel()
	type testCase struct {
		// PrevCommit is committed to a mark called "prev"
		PrevCommit map[string]string
		// NextCommit is committed to a mark called "next"
		NextCommit map[string]string
		// InFS (if not-nil) is what will be in the filesystem
		// When checkout is called to go from prev -> next
		// If InFS is nil, then PrevCommit is left unchanged before the call to checkout.
		InFS map[string]string
		// Err is the expected err returned from checking out NextCommit
		// when PrevCommit is checkout out currently
		Err error
	}
	tcs := []testCase{
		{
			PrevCommit: map[string]string{
				"a.txt": "file data a",
			},
			NextCommit: map[string]string{
				"a.txt": "file data a 2",
				"b.txt": "file data a 2",
			},
		},
		{
			PrevCommit: map[string]string{
				"a.txt": "file data a",
				"b.txt": "file data b",
				"c.txt": "file data c",
			},
			NextCommit: map[string]string{
				"a.txt": "file data a 2",
				"c.txt": "file data c 2",
			},
		},
		{
			PrevCommit: map[string]string{
				"a.txt": "file data a",
				"b.txt": "file data b",
				"c.txt": "file data c",
			},
			NextCommit: map[string]string{
				"a.txt": "file data a 2",
				"c.txt": "file data c 2",
			},
			InFS: map[string]string{
				"b.txt": "i made some changes, don't delete them",
			},
			Err: gotwc.ErrWouldClobber{
				Op:   "delete",
				Path: "b.txt",
			},
		},
	}
	for i, tc := range tcs {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			site := gottests.NewSite(t)
			// Commit some files to next
			site.CreateMark(gotrepo.FQM{Name: "next"})
			site.SetHead("next")
			site.WriteFSMap(tc.NextCommit)
			site.Put("")
			site.Commit(gotwc.CommitParams{})
			// clean them up
			for p := range tc.NextCommit {
				site.DeleteFile(p)
			}

			// Commit some different files to prev
			site.CreateMark(gotrepo.FQM{Name: "prev"})
			site.SetHead("prev")
			site.WriteFSMap(tc.PrevCommit)
			site.Put("")
			site.Commit(gotwc.CommitParams{})

			if tc.InFS != nil {
				for p := range tc.PrevCommit {
					if _, exists := tc.InFS[p]; !exists {
						site.DeleteFile(p)
					}
				}
				site.WriteFSMap(tc.InFS)
			}

			// Perform a checkout
			ctx := testutil.Context(t)
			err := site.WC.Checkout(ctx, "next")
			if tc.Err == nil {
				assert.NoError(t, err)
				// Check that the checkout was successful
				site.AssertFSEquals(tc.NextCommit)
			} else {
				assert.ErrorIs(t, err, tc.Err)
				// Check that the checkout did not happen.
				site.AssertFSEquals(tc.InFS)
			}
			site.CheckAll()
		})
	}
}

func TestExport(t *testing.T) {
	t.Parallel()
	type testCase struct {
		// InSnap is the data which will be the Commit to be exported.
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
			Err: gotwc.ErrWouldClobber{Op: "write", Path: "a.txt"},
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
