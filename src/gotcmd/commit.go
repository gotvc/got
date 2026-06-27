// command in this file operate on Commits
package gotcmd

import (
	"bufio"
	"fmt"
	"io"

	"github.com/gotvc/got/src/gdat"
	"github.com/gotvc/got/src/gotfs"
	"github.com/gotvc/got/src/gotrepo"
	"github.com/gotvc/got/src/internal/gotcore"
	"go.brendoncarroll.net/exp/streams"
	"go.brendoncarroll.net/star"
	"golang.org/x/sync/errgroup"
)

var historyCmd = star.Command{
	Metadata: star.Metadata{
		Short: "prints the commit log",
	},
	Pos: []star.Positional{fqmnOptParam},
	F: func(c star.Context) error {
		ctx := c.Context
		wc, err := openWC()
		if err != nil {
			return err
		}
		defer wc.Close()
		repo := wc.Repo()
		bname, err := wc.GetSaveTo()
		if err != nil {
			return err
		}
		fqm, ok := fqmnOptParam.LoadOpt(c)
		if !ok {
			fqm = gotrepo.FQM{Name: bname}
		}
		markExpr := gotcore.CommitExpr_Mark{Space: fqm.Space, Name: fqm.Name}
		pr, pw := io.Pipe()
		eg := errgroup.Group{}
		eg.Go(func() error {
			bufw := bufio.NewWriter(pw)
			err := repo.History(ctx, markExpr, func(ref gdat.Ref, comm gotrepo.Commit) error {
				if err := printcomm(bufw, ref, comm); err != nil {
					return err
				}
				if err := bufw.WriteByte('\n'); err != nil {
					return err
				}
				return bufw.Flush()
			})
			pw.CloseWithError(err)
			return err
		})
		eg.Go(func() error {
			err := pipeToLess(pr)
			// _, err := io.Copy(c.StdOut, pr)
			pr.CloseWithError(err)
			return err
		})
		return eg.Wait()
	},
}

func printcomm(bufw *bufio.Writer, ref gdat.Ref, comm gotcore.Commit) error {
	fmt.Fprintf(bufw, "#%04d\t%v\n", comm.N, ref.CID)
	fmt.Fprintf(bufw, "FS: %v\n", comm.Payload.Snap.Ref.CID)
	if len(comm.Parents) == 0 {
		fmt.Fprintf(bufw, "Parents: (none)\n")
	} else {
		fmt.Fprintf(bufw, "Parents:\n")
		for _, parent := range comm.Parents {
			fmt.Fprintf(bufw, "  %v\n", parent.CID)
		}
	}
	fmt.Fprintf(bufw, "Created At: %v\n", comm.CreatedAt.GoTime().Local().String())
	fmt.Fprintf(bufw, "Created By: %v\n", comm.Creator)
	bufw.Write([]byte(prettifyJSON(comm.Payload.Notes)))
	fmt.Fprintln(bufw)
	return nil
}

var diffCmd = star.Command{
	Metadata: star.Metadata{Short: "diff 2 commits. prints what must be applied to <left> to get <right>"},
	Pos:      []star.Positional{leftCE, rightCE},
	F: func(c star.Context) error {
		repo, closer, err := openRepo(c)
		if err != nil {
			return err
		}
		defer closer()
		w := bufio.NewWriter(c.StdOut)
		if err := repo.DiffFS(c, leftCE.Load(c), rightCE.Load(c), func(dfr *gotfs.Differ) error {
			var currentPath string
			return streams.ForEach(c, dfr, func(ent gotfs.DiffEntry) error {
				if ent.Left.Ok && ent.Right.Ok {
					return nil
				}
				if ent.Key.IsInfo() {
					currentPath = ent.Key.Path()
					var dir string
					if ent.Left.Ok && !ent.Right.Ok {
						dir = "-"
					} else if !ent.Left.Ok && ent.Right.Ok {
						dir = "+"
					}
					fmt.Fprintf(w, "%s %v\n", dir, currentPath)
				} else {
					if currentPath != ent.Key.Path() {
						currentPath = ent.Key.Path()
						fmt.Fprintf(w, "%s \n", currentPath)
					}
					var dir string
					var ext gotfs.Extent
					if ent.Left.Ok && !ent.Right.Ok {
						dir = "+"
						ext = ent.Left.X.Extent
					} else if !ent.Left.Ok && ent.Right.Ok {
						dir = "-"
						ext = ent.Right.X.Extent
					}
					l := ext.Length
					endAt := ent.Key.EndAt()
					startAt := endAt - uint64(l)
					fmt.Fprintf(w, "  %s [%v, %v) size=%vB \n", dir, startAt, endAt, l)
				}
				return nil
			})
		}); err != nil {
			return err
		}
		return w.Flush()
	},
}

var leftCE = &star.Required[gotcore.CommitExpr]{
	ShortDoc: "commit to be diffed",
	PosName:  "left-commit",
	Parse:    gotcore.ParseCommitExpr,
}

var rightCE = &star.Required[gotcore.CommitExpr]{
	ShortDoc: "commit to be diffed",
	PosName:  "right-commit",
	Parse:    gotcore.ParseCommitExpr,
}

var lsCmd = star.Command{
	Metadata: star.Metadata{
		Short: "lists the children of path in the current volume",
	},
	Flags: map[string]star.Flag{
		"comm": commExprOptParam,
	},
	Pos: []star.Positional{pathParam},
	F: func(c star.Context) error {
		ctx := c.Context
		wc, err := openWC()
		if err != nil {
			return err
		}
		defer wc.Close()
		se, ok := commExprOptParam.LoadOpt(c)
		if !ok {
			mname, err := wc.GetSaveTo()
			if err != nil {
				return err
			}
			se = &gotcore.CommitExpr_Mark{Name: mname}
		}
		p, _ := pathParam.LoadOpt(c)
		return wc.Repo().Ls(ctx, se, p, func(ent gotfs.DirEnt) error {
			_, err := fmt.Fprintf(c.StdOut, "%v %s\n", ent.Mode, ent.Name)
			return err
		})
	},
}

var catCmd = star.Command{
	Metadata: star.Metadata{
		Short: "writes the contents of path in the current volume to stdout",
	},
	Flags: map[string]star.Flag{
		"comm": commExprOptParam,
	},
	Pos: []star.Positional{pathParam},
	F: func(c star.Context) error {
		ctx := c.Context
		wc, err := openWC()
		if err != nil {
			return err
		}
		defer wc.Close()
		se, ok := commExprOptParam.LoadOpt(c)
		if !ok {
			mname, err := wc.GetSaveTo()
			if err != nil {
				return err
			}
			se = &gotcore.CommitExpr_Mark{Name: mname}
		}
		p, _ := pathParam.LoadOpt(c)
		return wc.Repo().Cat(ctx, se, p, c.StdOut)
	},
}

var commExprOptParam = &star.Optional[gotrepo.CommitExpr]{
	PosName:  "comm",
	Parse:    gotcore.ParseCommitExpr,
	ShortDoc: "a commit expression",
}

var fqmnOptParam = &star.Optional[gotrepo.FQM]{
	PosName: "mark-fq",
	Parse: func(s string) (gotrepo.FQM, error) {
		return gotrepo.ParseFQName(s), nil
	},
	ShortDoc: "a fully qualified mark name",
}

var pathParam = &star.Optional[string]{
	PosName: "path",
	Parse:   star.ParseString,
}
