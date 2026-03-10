You are implementing a change for this repository. Follow these steps in order without skipping any.

The change requested: $ARGUMENTS

---

## Step 1 — Detect the change type

Read the request and pick **one** type from the conventional commit standard. Use this mapping:

| Request mentions… | Type |
|---|---|
| bug, fix, broken, error, crash, wrong, incorrect, regression, issue | `fix` |
| doc, readme, documentation, example, guide, comment | `docs` |
| CI, workflow, pipeline, action, GitHub Actions, release, yml | `ci` |
| test, spec, coverage, assertion | `test` |
| refactor, cleanup, reorganise, reorganize, restructure, rename, move | `refactor` |
| chore, dependency, upgrade, bump, version, config, tooling | `chore` |
| performance, perf, speed, optimise, optimize | `perf` |
| anything else / new capability / feature | `feat` |

Keep the detected type in mind — use it for the branch prefix and commit message in every following step.

---

## Step 2 — Create a branch

Derive a short, kebab-case branch name from the request (lowercase, words separated by hyphens). Prefix it with the detected type.

```bash
git checkout main
git pull origin main
git checkout -b <type>/<short-description>
```

Confirm you are on the new branch before writing any code.

---

## Step 3 — Implement the change

**Write integration tests first, before any implementation code.** Run the tests to confirm they fail
(proving the feature is missing), then implement the feature, then run the tests again to confirm
they pass. This applies to all `feat` and `fix` changes; `test`-only changes skip the implementation sub-step.

Follow the project conventions in CLAUDE.md:
- Edit existing files rather than creating new ones where possible
- Keep changes minimal and focused — no unrelated cleanup
- Do not add comments, docstrings, or type annotations to code you did not change

---

## Step 4 — Commit

Stage only the files relevant to this change. Write a conventional commit message using the detected type.

```bash
git add <specific files>
git commit -m "<type>: <concise description in imperative mood>

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

---

## Step 5 — Push the branch

```bash
git push -u origin <type>/<short-description>
```

---

## Step 6 — Open a pull request

Use the detected type as the PR title prefix. Keep the title under 70 characters.

```bash
gh pr create \
  --title "<type>: <concise title>" \
  --body "$(cat <<'EOF'
## Summary
- <bullet summarising what changed and why>

## Test plan
- [ ] `cargo test` passes
- [ ] <specific behaviour to verify>

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

Return the PR URL to the user when done.
