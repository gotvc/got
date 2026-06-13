package gotcmd

import (
	"fmt"
	"os"
	"path/filepath"

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
}

var configAddPullCmd = star.Command{
	Metadata: star.Metadata{
		Short: "adds a pull task for a space",
	},
	Pos: []star.Positional{configSpaceNameParam},
	F: func(c star.Context) error {
		repo, err := openRepo()
		if err != nil {
			return err
		}
		defer repo.Close()
		spaceName := configSpaceNameParam.Load(c)
		return repo.Configure(func(x gotrepo.Config) gotrepo.Config {
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

func printConfig(c star.Context) error {
	workDir, err := os.OpenRoot(".")
	if err != nil {
		return err
	}
	defer workDir.Close()
	repoCfg, err := gotrepo.LoadConfig(workDir)
	if err != nil {
		return err
	}

	repoDir, _ := filepath.Abs(workDir.Name())
	c.Printf("REPO @ %s\n", repoDir)
	if err := printRepoConfig(&c, *repoCfg); err != nil {
		return err
	}

	wcCfg, err := gotwc.LoadConfig(workDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	wcDir, _ := filepath.Abs(workDir.Name())
	c.Printf("\nWORKING COPY @ %s\n", wcDir)
	c.Printf("  ID: %s\n", wcCfg.ID)
	c.Printf("  HEAD: %s\n", wcCfg.SaveTo)
	c.Printf("  ACT AS: %s\n", wcCfg.ActAs)
	c.Printf("  REPO DIR: %s\n", wcCfg.RepoDir)
	if len(wcCfg.Base) > 0 {
		c.Printf("  BASE:\n")
		for _, ref := range wcCfg.Base {
			c.Printf("  | %s\n", ref.CID)
		}
	}
	if len(wcCfg.Tracking) > 0 {
		c.Printf("  TRACKING:\n")
		for _, p := range wcCfg.Tracking {
			c.Printf("    %s\n", p)
		}
	}

	return nil
}

func printRepoConfig(c *star.Context, repoCfg gotrepo.Config) error {
	c.Printf("  REPO VOLUME: %s\n", repoCfg.RepoVolume)
	c.Printf("  BLOBCACHE:\n")
	if err := printBlobcacheConfig(c, repoCfg.Blobcache, "    "); err != nil {
		return err
	}

	if len(repoCfg.Identities) > 0 {
		c.Printf("  IDENTITIES:\n")
		for name, id := range repoCfg.Identities {
			c.Printf("  | %-30s %s\n", name, id.Base64String()[:16]+"...")
		}
	}

	if len(repoCfg.Spaces) > 0 {
		c.Printf("  SPACES:\n")
		c.Printf("    %-30s %-19s %-19s\n", "NAME", "NODE", "OID")
		for name, spec := range repoCfg.Spaces {
			if spec.Blobcache != nil {
				c.Printf("  | %-30s %16s... %16s...\n",
					name,
					spec.Blobcache.URL.Node.Base64String()[:16],
					spec.Blobcache.URL.Base.String()[:16],
				)
			}
		}
	}

	c.Printf("  PULL TASKS:\n")
	c.Printf("  |>%-20s %-20s %-20s %-20s\n", "FROM", "FILTER", "CUT_PREFIX", "ADD_PREFIX")
	for _, pc := range repoCfg.Pull {
		var filter string
		if pc.Filter != nil {
			filter = pc.Filter.String()
		}
		c.Printf("  | %-20s %-20s %-20s %-20s\n", pc.From, filter, pc.CutPrefix, pc.AddPrefix)
	}

	c.Printf("  PUSH TASKS:\n")
	c.Printf("  |>%-20s %-20s %-20s %-20s\n", "TO", "FILTER", "CUT_PREFIX", "ADD_PREFIX")
	for _, pc := range repoCfg.Push {
		var filter string
		if pc.Filter != nil {
			filter = pc.Filter.String()
		}
		c.Printf("  | %-20s %-20s %-20s %-20s\n", pc.To, filter, pc.CutPrefix, pc.AddPrefix)
	}
	return nil
}

func printBlobcacheConfig(c *star.Context, x gotrepo.BlobcacheSpec, indent string) error {
	switch {
	case x.EnvClient != nil:
		v, ok := os.LookupEnv(bcclient.EnvBlobcacheAPI)
		if !ok {
			v = bcclient.DefaultEndpoint + " (default)"
		}
		c.Printf(indent+"ENV: %v=%v\n", bcclient.EnvBlobcacheAPI, v)
	case x.InProcess != nil:
		ipcfg := x.InProcess
		c.Printf(indent + "IN PROCESS\n")
		c.Printf(indent+"ACT AS: \n", ipcfg.ActAs)
	default:
		c.Printf("  (EMPTY BLOBCACHE CONFIG)\n")
	}
	return nil
}
