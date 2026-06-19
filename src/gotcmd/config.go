package gotcmd

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	bcclient "blobcache.io/blobcache/client/go"
	"github.com/gotvc/got/src/gotrepo"
	"github.com/gotvc/got/src/gotwc"
	"go.brendoncarroll.net/star"
)

var configCmd = star.Command{
	Metadata: star.Metadata{
		Short: "prints the repo and working copy configuration",
	},
	F: func(c star.Context) error {
		if len(c.Extra) == 0 {
			return printConfig(c)
		}
		childName := c.Extra[0]
		rest := c.Extra[1:]
		child, ok := configSubCmds[childName]
		if !ok {
			return fmt.Errorf("no command found for %q", childName)
		}
		return star.Run(c.Context, child, c.Env, childName, rest, c.StdIn, c.StdOut, c.StdErr)
	},
}

var configSubCmds = map[string]star.Command{
	"add-pull": configAddPullCmd,
	"add-push": configAddPushCmd,
	"rm-pull":  configRmPullCmd,
	"rm-push":  configRmPushCmd,
}

var configAddPullCmd = star.Command{
	Metadata: star.Metadata{
		Short: "adds a pull task for a space",
	},
	Pos: []star.Positional{configSpaceNameParam},
	F: func(c star.Context) error {
		repo, close, err := openRepo()
		if err != nil {
			return err
		}
		defer close()
		spaceName := configSpaceNameParam.Load(c)
		return repo.Configure(c, func(x gotrepo.Config) gotrepo.Config {
			return *x.AddPull(gotrepo.PullConfig{
				From:      spaceName,
				AddPrefix: spaceName + "/",
			})
		})
	},
}

var configSpaceNameParam = &star.Required[string]{
	PosName: "space-name",
	Parse:   star.ParseString,
}

var configAddPushCmd = star.Command{
	Metadata: star.Metadata{
		Short: "adds a push task for a space",
	},
	Pos: []star.Positional{configSpaceNameParam},
	Flags: map[string]star.Flag{
		"add-prefix": configAddPrefixParam,
	},
	F: func(c star.Context) error {
		repo, close, err := openRepo()
		if err != nil {
			return err
		}
		defer close()
		spaceName := configSpaceNameParam.Load(c)
		return repo.Configure(c, func(x gotrepo.Config) gotrepo.Config {
			pc := gotrepo.PushConfig{
				To: spaceName,
			}
			if prefix, ok := configAddPrefixParam.LoadOpt(c); ok {
				pc.AddPrefix = prefix
			}
			x.Push = append(x.Push, pc)
			return x
		})
	},
}

var configAddPrefixParam = &star.Optional[string]{
	PosName:  "add-prefix",
	ShortDoc: "prefix to add to mark names before pushing",
	Parse:    star.ParseString,
}

var configRmPullCmd = star.Command{
	Metadata: star.Metadata{
		Short: "removes a pull task by index",
	},
	Pos: []star.Positional{configIndexParam},
	F: func(c star.Context) error {
		repo, close, err := openRepo()
		if err != nil {
			return err
		}
		defer close()
		i := configIndexParam.Load(c)
		return repo.Configure(c, func(x gotrepo.Config) gotrepo.Config {
			if i < 0 || i >= len(x.Pull) {
				return x
			}
			x.Pull = append(x.Pull[:i], x.Pull[i+1:]...)
			return x
		})
	},
}

var configRmPushCmd = star.Command{
	Metadata: star.Metadata{
		Short: "removes a push task by index",
	},
	Pos: []star.Positional{configIndexParam},
	F: func(c star.Context) error {
		repo, close, err := openRepo()
		if err != nil {
			return err
		}
		defer close()
		i := configIndexParam.Load(c)
		return repo.Configure(c, func(x gotrepo.Config) gotrepo.Config {
			if i < 0 || i >= len(x.Push) {
				return x
			}
			x.Push = append(x.Push[:i], x.Push[i+1:]...)
			return x
		})
	},
}

var configIndexParam = &star.Required[int]{
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

func printConfig(c star.Context) error {
	workDir, err := os.OpenRoot(".")
	if err != nil {
		return err
	}
	defer workDir.Close()
	dirpath, _ := filepath.Abs(workDir.Name())

	wcCfg, err := gotwc.LoadConfig(workDir)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	if err == nil {
		c.Printf("WORKING COPY @ %s\n", dirpath)
		if err := printWCConfig(&c, *wcCfg, "| "); err != nil {
			return err
		}
	}
	c.Printf("|" + strings.Repeat("_", 40) + "\n\n")

	repo, closer, err := openRepo()
	if err != nil {
		return err
	}
	defer closer()
	repoCfg := repo.Config()
	c.Printf("REPO @ %v\n", wcCfg.Repo)
	if err := printRepoConfig(&c, repoCfg, "| "); err != nil {
		return err
	}
	c.Printf("|" + strings.Repeat("_", 40) + "\n\n")

	return nil
}

func printWCConfig(c *star.Context, wcCfg gotwc.Config, indent string) error {
	c.Printf(indent+"ID: %s\n", wcCfg.ID)
	c.Printf(indent+"HEAD: %s\n", wcCfg.SaveTo)
	c.Printf(indent+"ACT AS: %s\n", wcCfg.ActAs)
	c.Printf(indent+"REPO: %v\n", wcCfg.Repo)
	if len(wcCfg.Base) > 0 {
		c.Printf(indent + "BASE:\n")
		for _, ref := range wcCfg.Base {
			c.Printf(indent+"| %s\n", ref.CID)
		}
	}
	if len(wcCfg.Tracking) > 0 {
		c.Printf(indent + "TRACKING:\n")
		for _, p := range wcCfg.Tracking {
			c.Printf("|   %s\n", p)
		}
	}
	c.Printf(indent + "BLOBCACHE:\n")
	if err := printBlobcacheConfig(c, wcCfg.Blobcache, "|   "); err != nil {
		return err
	}
	return nil
}

func printRepoConfig(c *star.Context, repoCfg gotrepo.Config, indent string) error {
	if len(repoCfg.Identities) > 0 {
		c.Printf(indent + "IDENTITIES:\n")
		for name, id := range repoCfg.Identities {
			c.Printf(indent+"|  %-30s %s\n", name, id.Base64String()[:16]+"...")
		}
	}

	if len(repoCfg.Spaces) > 0 {
		c.Printf(indent + "SPACES:\n")
		c.Printf(indent+"|  %-30s %-19s %-19s\n", "NAME", "NODE", "OID")
		for name, spec := range repoCfg.Spaces {
			if spec.Blobcache != nil {
				c.Printf(indent+"|  %-30s %16s... %16s...\n",
					name,
					spec.Blobcache.URL.Node.Base64String()[:16],
					spec.Blobcache.URL.Base.String()[:16],
				)
			}
		}
	}

	c.Printf(indent + "PULL TASKS:\n")
	c.Printf(indent+"|  %-20s %-20s %-20s %-20s\n", "FROM", "FILTER", "CUT_PREFIX", "ADD_PREFIX")
	for _, pc := range repoCfg.Pull {
		var filter string
		if pc.Filter != nil {
			filter = pc.Filter.String()
		} else {
			filter = "(none)"
		}
		c.Printf(indent+"|  %-20s %-20s %-20s %-20s\n", pc.From, filter, pc.CutPrefix, pc.AddPrefix)
	}

	c.Printf(indent + "PUSH TASKS:\n")
	c.Printf(indent+"|  %-20s %-20s %-20s %-20s\n", "TO", "FILTER", "CUT_PREFIX", "ADD_PREFIX")
	for _, pc := range repoCfg.Push {
		var filter string
		if pc.Filter != nil {
			filter = pc.Filter.String()
		} else {
			filter = "(none)"
		}
		c.Printf(indent+"|  %-20s %-20s %-20s %-20s\n", pc.To, filter, pc.CutPrefix, pc.AddPrefix)
	}
	return nil
}

func printBlobcacheConfig(c *star.Context, x gotwc.BlobcacheSpec, indent string) error {
	switch {
	case x.EnvClient != nil:
		v, ok := os.LookupEnv(bcclient.EnvBlobcacheAPI)
		if !ok {
			v = bcclient.DefaultEndpoint + " (default)"
		}
		c.Printf(indent+"  FROM-ENV: %v=%v\n", bcclient.EnvBlobcacheAPI, v)
	case x.InProcess != nil:
		c.Printf(indent + "  IN PROCESS\n")
	default:
		c.Printf(indent + "  (EMPTY BLOBCACHE CONFIG)\n")
	}
	return nil
}
