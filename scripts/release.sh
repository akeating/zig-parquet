#!/usr/bin/env bash
set -e

if [ -z "$1" ]; then
    echo "Usage: ./scripts/release.sh <version>"
    echo "Example: ./scripts/release.sh 0.2.0"
    exit 1
fi

VERSION=$1

if ! [[ "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[a-zA-Z0-9.]+)?$ ]]; then
    echo "Error: '$VERSION' is not valid semver (expected X.Y.Z or X.Y.Z-prerelease)"
    exit 1
fi

if git rev-parse "v$VERSION" >/dev/null 2>&1; then
    echo "Error: tag v$VERSION already exists"
    exit 1
fi


if [ -n "$(git status --porcelain)" ]; then
    echo "Error: working tree is not clean. Commit or stash changes first."
    exit 1
fi

echo "Releasing version v$VERSION..."

# Update version in both build.zig.zon files
sed -i.bak -E "s/\.version = \"[^\"]+\"/.version = \"$VERSION\"/" zig-parquet/build.zig.zon
rm -f zig-parquet/build.zig.zon.bak

sed -i.bak -E "s/\.version = \"[^\"]+\"/.version = \"$VERSION\"/" cli/build.zig.zon
rm -f cli/build.zig.zon.bak

# Update README.md
# This looks for the GitHub archive URL and replaces the tag portion
sed -i.bak -E "s/refs\/tags\/v[0-9]+\.[0-9]+\.[0-9]+(-[a-zA-Z0-9.]+)?\.tar\.gz/refs\/tags\/v$VERSION.tar.gz/g" README.md
rm -f README.md.bak

echo "✅ Updated build.zig.zon and README.md"
echo ""
echo "Changes:"
git --no-pager diff

echo ""
echo "If everything looks good, commit and push to trigger the release workflow:"
echo "  git commit -am \"Release v$VERSION\""
echo "  git tag v$VERSION"
echo "  git push origin main --tags"
echo ""
echo "CI will cross-compile pqi for all platforms and create the GitHub Release."
