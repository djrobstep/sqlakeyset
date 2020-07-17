#!/bin/bash
set -e

current=$(git rev-parse --abbrev-ref HEAD)
tag=$(git describe --always)
staging=$(mktemp -d)

make docs
mv doc/build/html/* $staging/

git branch -D gh-pages || true

# git-checkout manpage recommends this for creating an empty commit:
git checkout --orphan gh-pages
git rm -rf .
# remove any ignored files:
rm -rf *
mv $staging/* .
git add -f *
git commit -m "gh-pages build for $tag"
git checkout $current
