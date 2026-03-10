Merge a pull request and sync the local main branch.

PR to merge: $ARGUMENTS

---

## Step 1 — Merge the PR

Use the GitHub CLI to merge the PR with a squash merge (project convention):

```bash
gh pr merge $ARGUMENTS --squash --delete-branch
```

If `$ARGUMENTS` is empty, list open PRs first so the user can pick one:

```bash
gh pr list
```

Then prompt the user for the PR number before proceeding.

---

## Step 2 — Sync local main

```bash
git checkout main
git pull origin main
```

---

## Step 3 — Confirm

Report the latest commit on main (hash + message) so the user can confirm the merge landed:

```bash
git log --oneline -1
```
