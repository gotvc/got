package gotcmd

import (
	"github.com/gotvc/got/pkg/gotrepo"
	"github.com/gotvc/got/pkg/metrics"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

func newAddCmd(open func() (*gotrepo.Repo, error)) *cobra.Command {
	var repo *gotrepo.Repo
	return &cobra.Command{
		Use:     "add",
		Short:   "adds paths to the staging area",
		PreRunE: loadRepo(&repo, open),
		Args:    cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) < 1 {
				return errors.Errorf("path argument required")
			}
			r := metrics.NewTTYRenderer(collector, cmd.OutOrStdout())
			defer r.Close()
			return repo.Add(ctx, args...)
		},
	}
}

func newRmCmd(open func() (*gotrepo.Repo, error)) *cobra.Command {
	var repo *gotrepo.Repo
	return &cobra.Command{
		Use:     "rm",
		Short:   "stages paths for deletion",
		PreRunE: loadRepo(&repo, open),
		Args:    cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) < 1 {
				return errors.Errorf("path argument required")
			}
			r := metrics.NewTTYRenderer(collector, cmd.OutOrStdout())
			defer r.Close()
			return repo.Rm(ctx, args...)
		},
	}
}

func newPutCmd(open func() (*gotrepo.Repo, error)) *cobra.Command {
	var repo *gotrepo.Repo
	return &cobra.Command{
		Use:     "put",
		Short:   "stages paths for replacement",
		PreRunE: loadRepo(&repo, open),
		Args:    cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) < 1 {
				return errors.Errorf("path argument required")
			}
			r := metrics.NewTTYRenderer(collector, cmd.OutOrStdout())
			defer r.Close()
			return repo.Put(ctx, args...)
		},
	}
}

func newDiscardCmd(open func() (*gotrepo.Repo, error)) *cobra.Command {
	var repo *gotrepo.Repo
	return &cobra.Command{
		Use:     "discard",
		Short:   "discards any changes staged for a path",
		PreRunE: loadRepo(&repo, open),
		Args:    cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) < 1 {
				return errors.Errorf("path argument required")
			}
			return repo.Discard(ctx, args...)
		},
	}
}

func newClearCmd(open func() (*gotrepo.Repo, error)) *cobra.Command {
	var repo *gotrepo.Repo
	return &cobra.Command{
		Use:     "clear",
		Short:   "clears the staging area",
		PreRunE: loadRepo(&repo, open),
		Args:    cobra.ExactArgs(0),
		RunE: func(cmd *cobra.Command, args []string) error {
			return repo.Clear(ctx)
		},
	}
}
