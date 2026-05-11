#!/bin/bash

set -e  # Exit on error

# --- Helper functions ---

function error_exit {
  echo "❌ $1"
  exit 1
}

function pause {
  read -p "$1" REPLY
  echo "$REPLY"
}

# --- Pre-checks ---

# Ensure working directory is clean
if ! git diff-index --quiet HEAD --; then
  error_exit "You have uncommitted changes. Commit or stash them before releasing."
fi

# Run tests
echo "🔍 Running test suite..."
if command -v uv &> /dev/null; then
  uv run --extra dev pytest -v || error_exit "Tests failed. Aborting release."
else
  pytest -v || error_exit "Tests failed. Aborting release."
fi

# Get version from setuptools_scm
AUTO_VERSION=$(python -c "import setuptools_scm; print(setuptools_scm.get_version())") \
  || error_exit "Could not determine version from setuptools_scm."
DEFAULT_TAG="v$AUTO_VERSION"

# Confirm or override version tag
read -p "Enter version tag to release [default: $DEFAULT_TAG]: " VERSION
VERSION="${VERSION:-$DEFAULT_TAG}"

# Check if tag exists
if git rev-parse "$VERSION" >/dev/null 2>&1; then
  error_exit "Git tag $VERSION already exists. Use a new version or delete the tag."
fi

# Confirm upload target
TARGET=$(pause "Upload to (1) TestPyPI or (2) PyPI? [1/2]: ")
if [[ "$TARGET" != "1" && "$TARGET" != "2" ]]; then
  error_exit "Invalid target. Must be 1 or 2."
fi

# Check PyPI credentials
if [ ! -f ~/.pypirc ]; then
  error_exit "~/.pypirc not found. Please set up your PyPI credentials."
fi

# --- Tag and push ---
git tag "$VERSION"
git push origin "$VERSION"

# --- Clean builds ---
rm -rf dist/ build/ *.egg-info

# --- Build & Upload the package (from the exact tag) ---
echo "🔧 Building package..."
python -m build

if [[ $TARGET == "1" ]]; then
  echo "🚀 Uploading to TestPyPI..."
  python -m twine upload --repository testpypi dist/*
  URL="https://test.pypi.org/project/pymaap/"
else
  echo "🚀 Uploading to PyPI..."
  python -m twine upload dist/*
  URL="https://pypi.org/project/pymaap/"
fi

echo "✅ Package built & uploaded: $URL"

# --- Generate changelog ---
if command -v git-cliff &> /dev/null && [ -f .gitcliff.toml ]; then
  echo "📝 Generating CHANGELOG.md..."
  git-cliff -c .gitcliff.toml -o CHANGELOG.md
else
  echo "⚠️  Skipping changelog: git-cliff not found or config missing."
fi

# --- Auto-commit CHANGELOG.md ---
if [ -f CHANGELOG.md ]; then
  git add CHANGELOG.md
  git commit -m "chore: update changelog for $VERSION"
  git push origin main
  echo "✅ CHANGELOG.md committed and pushed."
fi

# --- Final success + open URL ---
echo "✅ Release complete: $VERSION"
echo "🌐 View your package at:"
echo "$URL"

if command -v open &>/dev/null; then
  open "$URL"
elif command -v xdg-open &>/dev/null; then
  xdg-open "$URL"
fi