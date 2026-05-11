# Releasing PyMAAP to PyPI

This document describes how to ship a **tagged** release with an up-to-date **CHANGELOG** and wheels/sdists that match that tag.

## Prerequisites

- **Python** 3.10+ (matches `requires-python` in `pyproject.toml`).
- **Clean git tree** on `main` (commit or stash local changes).
- **CI green** on the commit you intend to release ([GitHub Actions](https://github.com/sralter/pymaap/actions)).
- **Tools:** `git`, `git-cliff` (for `CHANGELOG.md`), and either **`uv`** (recommended) or a system **`python3`** with `pytest`, `build`, `twine`, and **`setuptools-scm`** installed for release commands.
- **PyPI credentials** — either:
  - `~/.pypirc` with a **`[pypi]`** section for production uploads, or
  - **API tokens:** set `TWINE_USERNAME=__token__` and `TWINE_PASSWORD=<token>` when you run the script. `release.sh` allows starting a release if `~/.pypirc` exists **or** `TWINE_PASSWORD` is set.
- **TestPyPI** uses a **separate** token from PyPI. `./release.sh` uploads to TestPyPI with **`twine --repository-url https://test.pypi.org/legacy/`** (so you do **not** need a `[testpypi]` section in `~/.pypirc`). Export a [TestPyPI API token](https://test.pypi.org/manage/account/token/) in `TWINE_PASSWORD` (and `TWINE_USERNAME=__token__`) when you choose option **1**, or add a dedicated `[testpypi]` section if you prefer named repositories.

## Versioning (`setuptools_scm`)

- The **distribution version** is computed from **git tags** matching `v[0-9]*` (see `[tool.setuptools_scm]` in `pyproject.toml` and `tag_pattern` in `.gitcliff.toml`).
- Choose a tag that reflects **semver** for your changes (e.g. `v0.2.0` after breaking changes such as raising `requires-python` or multiprocessing behavior).
- Sanity-check the version Python would use **before** tagging (use the project dev environment so `setuptools_scm` resolves):

  ```bash
  uv run --extra dev python -c "import setuptools_scm; print(setuptools_scm.get_version())"
  ```

  Without `uv`, use `python3 -c "..."` if your environment has **`setuptools-scm`** installed (e.g. `pip install -e '.[dev]'`).

  On a commit **without** the new tag, this may show a development/post string; after you tag `vX.Y.Z` on the release commit, builds from that tag resolve to `X.Y.Z`.

## Conventional commits and changelog

- **git-cliff** groups [Conventional Commits](https://www.conventionalcommits.org/) (`feat:`, `fix:`, `docs:`, `chore:`, etc.).
- Use **`feat!:`** / **`fix!:`** or a **`BREAKING CHANGE:`** footer for incompatible changes so they appear clearly in generated notes.
- Regenerate locally anytime:

  ```bash
  git-cliff -c .gitcliff.toml -o CHANGELOG.md
  ```

## Recommended flow (changelog on the tag, artifact matches)

This is what **`./release.sh`** is wired to do: **regenerate `CHANGELOG.md`, commit it to `main`, then create and push the tag, then build and upload.** The sdist/wheel on PyPI then include the changelog commit that documents the release.

1. `git checkout main && git pull`
2. Ensure all work is merged and **tests pass** (`uv run --extra dev pytest -v`).
3. Run **`./release.sh`** from the repo root (see script for interactive prompts). When **`uv`** is on your `PATH`, the script uses **`uv run --extra dev python`** for version detection, `build`, and `twine` so you do not need a global `python` shim.
4. When prompted, choose **TestPyPI first** (`1`) for a dry run if you want, then **PyPI** (`2`) for the real upload.
5. **Optional:** On GitHub, open **Releases → New release**, select the tag, paste the section of `CHANGELOG.md` for that version, and attach `dist/*` if you want artifacts on GitHub.

## Manual flow (same order as the script)

If you prefer not to use the script:

```bash
# 1. Clean tree + tests
uv run --extra dev pytest -v

# 2. Changelog (edit CHANGELOG.md after if needed)
git-cliff -c .gitcliff.toml -o CHANGELOG.md
git add CHANGELOG.md
git commit -m "chore: update changelog for vX.Y.Z"
git push origin main

# 3. Tag the release commit
git tag vX.Y.Z
git push origin vX.Y.Z

# 4. Build from that tag (strict: checkout tag first)
git fetch origin && git checkout vX.Y.Z
rm -rf dist/ build/ *.egg-info
uv run --extra dev python -m build
uv run --extra dev python -m twine check dist/*

# 5. Upload (TestPyPI: explicit URL avoids needing [testpypi] in ~/.pypirc)
uv run --extra dev python -m twine upload --repository-url https://test.pypi.org/legacy/ dist/*
#    PyPI:
# uv run --extra dev python -m twine upload dist/*
```

## `release.sh` behavior summary

- Aborts on **dirty** working tree or **failing tests**.
- Uses **`setuptools_scm`** to suggest a default **tag**; you may override it at the prompt.
- Refuses to create a tag that **already exists**.
- Runs **`git-cliff`** (if installed), **commits** `CHANGELOG.md` when it changed, **pushes `main`**, then **tags** and **pushes the tag**, then **`build` / `twine`** via **`uv run --extra dev python -m …`** when `uv` is available, otherwise **`python3`** (or **`python`**).
- **TestPyPI** uploads use **`--repository-url https://test.pypi.org/legacy/`** (not `--repository testpypi`), so a `[testpypi]` stanza in `~/.pypirc` is optional.
- If **`git-cliff`** is missing, it skips changelog generation (you should run it manually before release).

## Notes

- **GitHub Pages / MkDocs** are separate from PyPI; update docs when you want the site to match the release.
- Personal release notes can live in **`README_release.md`** at the repo root (tracked); the canonical checklist is this file.
