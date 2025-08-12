# Scripts

This folder contains helper scripts and tools.

## Importing issues from CSV (gh-import-issues.py)

This repo includes a helper script to import issues via GitHub CLI:

- Script: `scripts/gh-import-issues.py`
- CSV template: `scripts/issues-template.csv`

Prereqs:

- Install and login to GitHub CLI

```bash
gh auth login
# If you plan to add items to Projects (v2):
gh auth refresh -s project
```

Dry-run and then import:

```bash
# Preview actions without making changes
python3 scripts/gh-import-issues.py --csv scripts/issues-template.csv --dry-run

# Create issues in the current repo context
python3 scripts/gh-import-issues.py --csv scripts/issues-template.csv

# Or target a specific repo
python3 scripts/gh-import-issues.py --csv /path/to/your.csv --repo OWNER/REPO
```

CSV columns (case-insensitive): `title, body, labels, assignees, milestone, project_owner, project_number, state, close_reason`. Labels and assignees are semicolon-delimited within the cell; missing labels and milestones can be auto-created.
