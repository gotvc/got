#!/bin/bash

# Use --allow-dirty as a parameter to skip the clean tree check
if [[ "$1" != "--allow-dirty" ]]; then
  # Check if the git working tree is clean
  if [ -n "$(git status --porcelain)" ]; then
    echo "Error: Working tree is not clean."
    exit 1
  fi
fi

# Get the git hash and print it
git_hash=$(git rev-parse --short=7 HEAD)
echo "git-$git_hash"

# Print all tags pointing at the current HEAD
git_tag_at_head=$(git tag --points-at HEAD)
if [ -n "$git_tag_at_head" ]; then
  echo "$git_tag_at_head"
fi

echo "latest"
