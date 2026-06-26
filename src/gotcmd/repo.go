package gotcmd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strconv"

	bcclient "blobcache.io/blobcache/client/go"
	"blobcache.io/blobcache/src/blobcache"
	_ "blobcache.io/blobcache/src/blobcachecmd"
	"github.com/gotvc/got/src/gotrepo"
	"go.brendoncarroll.net/star"
)

var repoCmd = star.NewDir(star.Metadata{
	Short: "repo maintenance commands",
}, map[string]star.Command{
	"repair-links": repairLinksCmd,
	"init":         repoInitCmd,
	"create":       repoCreateCmd,
	"add-push":     repoAddPushCmd,
	"add-pull":     repoAddPullCmd,
	"rm-push":      repoRmPushCmd,
	"rm-pull":      repoRmPullCmd,
	"edit":         repoEditCmd,
})

var repoInitCmd = star.Command{
	Metadata: star.Metadata{
		Short: "initialize a Repo in an existing Blobcache Volume.  Does not create a working copy.",
	},
	Pos: []star.Positional{volNameParam},
	F: func(c star.Context) error {
		ctx := c.Context
		bc := bcclient.NewClientFromEnv()
		nsc, err := bcclient.NewNSClientFromEnv()
		if err != nil {
			return err
		}
		volh, err := nsc.Open(ctx, volNameParam.Load(c), blobcache.Action_ALL)
		if err != nil {
			return err
		}
		if err := gotrepo.Init(ctx, bc, volh, gotrepo.DefaultConfig()); err != nil {
			return err
		}
		c.Printf("repo initialized successfully")
		return nil
	},
}

var repoEditCmd = star.Command{
	Metadata: star.Metadata{
		Short: "edit the repo config in your editor",
	},
	F: func(c star.Context) error {
		repo, close, err := openRepo(c)
		if err != nil {
			return err
		}
		defer close()
		editor := os.Getenv("EDITOR")
		if editor == "" {
			editor = "vi"
		}
		if err := repo.Configure(c, func(x gotrepo.Config) (gotrepo.Config, error) {
			if err := userEdit(c.Context, &x, editor); err != nil {
				return x, err
			}
			return x, nil
		}); err != nil {
			return err
		}
		c.Printf("config saved successfully\n")
		return nil
	},
}

func userEdit[T any](ctx context.Context, x *T, editor string) error {
	data, err := json.MarshalIndent(x, "", "  ")
	if err != nil {
		return err
	}
	f, err := os.CreateTemp("", "got-edit-*.json")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	defer f.Close()
	if _, err := f.Write(data); err != nil {
		return err
	}
	for {
		cmd := exec.Command(editor, f.Name())
		cmd.Stdin = os.Stdin
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err != nil {
			return fmt.Errorf("editor: %w", err)
		}
		data, err := os.ReadFile(f.Name())
		if err != nil {
			return err
		}
		if err := json.Unmarshal(data, x); err != nil {
			fmt.Fprintf(os.Stderr, "invalid JSON: %v\n", err)
			continue
		}
		return nil
	}
}

var repoCreateCmd = star.Command{
	Metadata: star.Metadata{
		Short: "create a new Volume and initialize a Repo in it.",
	},
	Pos: []star.Positional{volNameParam},
	F: func(c star.Context) error {
		ctx := c.Context
		svc := bcclient.NewClientFromEnv()
		ep, err := svc.Endpoint(ctx)
		if err != nil {
			return err
		}
		volName := volNameParam.Load(c)
		volh, err := createRepoVol(ctx, svc, volName)
		if err != nil {
			return err
		}
		spec := gotrepo.RepoVolumeSpec(false)
		specJSON, err := json.Marshal(spec)
		if err != nil {
			return err
		}
		c.Printf("Successfully created a new volume in the ns root\n")
		c.Printf("NODE: %v", ep.Node)
		c.Printf("HANDLE: %v\n", volh)
		c.Printf("INFO: %s\n", prettifyJSON(specJSON))

		c.Printf("initializing volume...\n")
		if err := gotrepo.Init(ctx, svc, volh, gotrepo.DefaultConfig()); err != nil {
			return err
		}
		c.Printf("successfully initialized Repo in %v", volh.OID)
		return nil
	},
}

var volNameParam = &star.Required[string]{
	PosName:  "vol-name",
	Parse:    star.ParseString,
	ShortDoc: "the name in the namespace to use for the new volume",
}

var scrubCmd = star.Command{
	Metadata: star.Metadata{
		Short: "runs integrity checks",
	},
	F: func(c star.Context) error {
		ctx := c.Context
		repo, close, err := openRepo(c)
		if err != nil {
			return err
		}
		defer close()
		if err := repo.CheckAll(ctx); err != nil {
			return err
		}
		c.Printf("everything is OK\n")
		return nil
	},
}

var repairLinksCmd = star.Command{
	Metadata: star.Metadata{
		Short: "repairs repo volume link tokens",
	},
	F: func(c star.Context) error {
		repo, close, err := openRepo(c)
		if err != nil {
			return err
		}
		defer close()
		if err := gotrepo.RepairRepoLinks(c.Context, repo); err != nil {
			return err
		}
		c.Printf("repaired repo links\n")
		return nil
	},
}

var repoAddPullCmd = star.Command{
	Metadata: star.Metadata{
		Short: "adds a pull task for a space",
	},
	Pos: []star.Positional{configSpaceNameParam},
	F: func(c star.Context) error {
		repo, close, err := openRepo(c)
		if err != nil {
			return err
		}
		defer close()
		spaceName := configSpaceNameParam.Load(c)
		return repo.Configure(c, func(x gotrepo.Config) (gotrepo.Config, error) {
			return *x.AddPull(gotrepo.PullConfig{
				From:      spaceName,
				AddPrefix: spaceName + "/",
			}), nil
		})
	},
}

var configSpaceNameParam = &star.Required[string]{
	PosName: "space-name",
	Parse:   star.ParseString,
}

var repoAddPushCmd = star.Command{
	Metadata: star.Metadata{
		Short: "adds a push task for a space",
	},
	Pos: []star.Positional{configSpaceNameParam},
	Flags: map[string]star.Flag{
		"add-prefix": addPrefixParam,
	},
	F: func(c star.Context) error {
		repo, close, err := openRepo(c)
		if err != nil {
			return err
		}
		defer close()
		spaceName := configSpaceNameParam.Load(c)
		return repo.Configure(c, func(x gotrepo.Config) (gotrepo.Config, error) {
			pc := gotrepo.PushConfig{
				To: spaceName,
			}
			if prefix, ok := addPrefixParam.LoadOpt(c); ok {
				pc.AddPrefix = prefix
			}
			x.Push = append(x.Push, pc)
			return x, nil
		})
	},
}

var repoRmPullCmd = star.Command{
	Metadata: star.Metadata{
		Short: "removes a pull task by index",
	},
	Pos: []star.Positional{taskIndexParam},
	F: func(c star.Context) error {
		repo, close, err := openRepo(c)
		if err != nil {
			return err
		}
		defer close()
		i := taskIndexParam.Load(c)
		return repo.Configure(c, func(x gotrepo.Config) (gotrepo.Config, error) {
			if i < 0 || i >= len(x.Pull) {
				return x, nil
			}
			x.Pull = append(x.Pull[:i], x.Pull[i+1:]...)
			return x, nil
		})
	},
}

var repoRmPushCmd = star.Command{
	Metadata: star.Metadata{
		Short: "removes a push task by index",
	},
	Pos: []star.Positional{taskIndexParam},
	F: func(c star.Context) error {
		repo, close, err := openRepo(c)
		if err != nil {
			return err
		}
		defer close()
		i := taskIndexParam.Load(c)
		return repo.Configure(c, func(x gotrepo.Config) (gotrepo.Config, error) {
			if i < 0 || i >= len(x.Push) {
				return x, nil
			}
			x.Push = append(x.Push[:i], x.Push[i+1:]...)
			return x, nil
		})
	},
}

var taskIndexParam = &star.Required[int]{
	PosName:  "index",
	ShortDoc: "the index of the task to remove",
	Parse: func(s string) (int, error) {
		i, err := strconv.Atoi(s)
		if err != nil {
			return 0, fmt.Errorf("invalid index: %q", s)
		}
		return i, nil
	},
}
