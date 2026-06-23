package gotcmd

import (
	"fmt"
	"os"
	"path/filepath"
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

var configSubCmds = map[string]star.Command{}

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
	c.Printf("%s\n\n", "|"+strings.Repeat("_", 40))

	repo, closer, err := openRepo(c)
	if err != nil {
		return err
	}
	defer closer()
	repoCfg := repo.Config()
	c.Printf("REPO @ %v\n", wcCfg.Repo)
	if err := printRepoConfig(&c, repoCfg, "| "); err != nil {
		return err
	}
	c.Printf("%s\n\n", "|"+strings.Repeat("_", 40))

	return nil
}

func printWCConfig(c *star.Context, wcCfg gotwc.Config, indent string) error {
	c.Printf("%sID: %s\n", indent, wcCfg.ID)
	c.Printf("%sHEAD: %s\n", indent, wcCfg.SaveTo)
	c.Printf("%sACT AS: %s\n", indent, wcCfg.ActAs)
	c.Printf("%sREPO: %v\n", indent, wcCfg.Repo)
	if len(wcCfg.Base) > 0 {
		c.Printf("%sBASE:\n", indent)
		for _, ref := range wcCfg.Base {
			c.Printf("%s| %s\n", indent, ref.CID)
		}
	}
	if len(wcCfg.Tracking) > 0 {
		c.Printf("%sTRACKING:\n", indent)
		for _, p := range wcCfg.Tracking {
			c.Printf("|   %s\n", p)
		}
	}
	c.Printf("%sBLOBCACHE:\n", indent)
	if err := printBlobcacheConfig(c, wcCfg.Blobcache, "|   "); err != nil {
		return err
	}
	return nil
}

func printRepoConfig(c *star.Context, repoCfg gotrepo.Config, indent string) error {
	if len(repoCfg.Identities) > 0 {
		c.Printf("%sIDENTITIES:\n", indent)
		for name, id := range repoCfg.Identities {
			c.Printf("%s|  %-30s %s\n", indent, name, id.Base64String()[:16]+"...")
		}
	}

	if len(repoCfg.Spaces) > 0 {
		c.Printf("%sSPACES:\n", indent)
		c.Printf("%s|  %-30s %-19s %-19s\n", indent, "NAME", "NODE", "OID")
		for name, spec := range repoCfg.Spaces {
			if spec.Blobcache != nil {
				c.Printf("%s|  %-30s %16s... %16s...\n",
					indent,
					name,
					spec.Blobcache.URL.Node.Base64String()[:16],
					spec.Blobcache.URL.OID.String()[:16],
				)
			}
		}
	}

	c.Printf("%sPULL TASKS:\n", indent)
	c.Printf("%s|  %-20s %-20s %-20s %-20s\n", indent, "FROM", "FILTER", "CUT_PREFIX", "ADD_PREFIX")
	for _, pc := range repoCfg.Pull {
		var filter string
		if pc.Filter != nil {
			filter = pc.Filter.String()
		} else {
			filter = "(none)"
		}
		c.Printf("%s|  %-20s %-20s %-20s %-20s\n", indent, pc.From, filter, pc.CutPrefix, pc.AddPrefix)
	}

	c.Printf("%sPUSH TASKS:\n", indent)
	c.Printf("%s|  %-20s %-20s %-20s %-20s\n", indent, "TO", "FILTER", "CUT_PREFIX", "ADD_PREFIX")
	for _, pc := range repoCfg.Push {
		var filter string
		if pc.Filter != nil {
			filter = pc.Filter.String()
		} else {
			filter = "(none)"
		}
		c.Printf("%s|  %-20s %-20s %-20s %-20s\n", indent, pc.To, filter, pc.CutPrefix, pc.AddPrefix)
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
		c.Printf("%s  FROM-ENV: %v=%v\n", indent, bcclient.EnvBlobcacheAPI, v)
	case x.InProcess != nil:
		c.Printf("%s  IN PROCESS\n", indent)
	default:
		c.Printf("%s  (EMPTY BLOBCACHE CONFIG)\n", indent)
	}
	return nil
}
