#!/usr/bin/env python3
"""
Import GitHub Issues from a CSV using GitHub CLI (gh).

Features
- Robust CSV parsing via Python csv module (UTF-8)
- Auto-create missing labels (gh label create)
- Auto-create missing milestones via REST (gh api)
- Assign multiple labels/assignees per row
- Optionally add created issues to a GitHub Project (Projects v2) via gh project item-add
- Optionally close issues after creation with a reason
- Dry-run mode and per-issue delay to avoid rate limits

Requirements
- gh CLI installed and authenticated: `gh auth login`
- For Projects (v2): grant scope: `gh auth refresh -s project`

CSV columns (header names are case-insensitive)
- title (required)
- body (optional)
- labels (optional; semicolon-separated within the cell)
- assignees (optional; semicolon-separated GitHub logins)
- milestone (optional; milestone title; will be created if missing)
- project_owner (optional; org/user login for Projects v2)
- project_number (optional; project number for Projects v2)
- state (optional; "open" or "closed"; default "open")
- close_reason (optional; when state is closed: one of "completed" | "not planned")

Notes
- If labels contain commas, wrap the field in quotes in the CSV. Inside the cell, use `;` to separate multiple labels.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import subprocess
import sys
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
import math


# -------------------------- Utilities --------------------------

def run(cmd: List[str], input_bytes: Optional[bytes] = None) -> Tuple[int, str, str]:
    """Run a command and return (code, stdout, stderr)."""
    proc = subprocess.Popen(cmd, stdin=subprocess.PIPE if input_bytes else None, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = proc.communicate(input=input_bytes)
    return proc.returncode, out.decode("utf-8", errors="replace"), err.decode("utf-8", errors="replace")


def gh_json(cmd: List[str]) -> object:
    code, out, err = run(cmd)
    if code != 0:
        raise RuntimeError(f"Command failed: {' '.join(cmd)}\n{err}")
    try:
        return json.loads(out)
    except json.JSONDecodeError as e:
        raise RuntimeError(f"Failed to parse JSON from: {' '.join(cmd)}\nOutput was:\n{out}\nError: {e}")


def ensure_repo_name_with_owner(repo: Optional[str]) -> str:
    """Resolve repo in owner/name format using gh if not provided."""
    if repo:
        return repo
    # Query current repository context
    data = gh_json(["gh", "repo", "view", "--json", "nameWithOwner"])
    if not isinstance(data, dict) or "nameWithOwner" not in data:
        raise RuntimeError("Could not resolve current repository via `gh repo view`. Pass --repo.")
    return data["nameWithOwner"]


def split_list(cell: str) -> List[str]:
    if not cell:
        return []
    # Semicolon as list separator to avoid conflict with CSV commas
    parts = [p.strip() for p in cell.split(";")]
    return [p for p in parts if p]


# -------------------------- Data types --------------------------

@dataclass
class IssueRow:
    title: str
    body: str = ""
    labels: List[str] = None  # type: ignore
    assignees: List[str] = None  # type: ignore
    milestone: Optional[str] = None
    project_owner: Optional[str] = None
    project_number: Optional[int] = None
    state: str = "open"
    close_reason: Optional[str] = None

    def __post_init__(self):
        if self.labels is None:
            self.labels = []
        if self.assignees is None:
            self.assignees = []


# -------------------------- gh helpers --------------------------

def get_existing_labels(repo: str) -> List[str]:
    cmd = ["gh", "label", "list", "--json", "name", "-R", repo]
    data = gh_json(cmd)
    if not isinstance(data, list):
        return []
    return [item.get("name", "") for item in data if isinstance(item, dict)]


def ensure_labels(repo: str, labels: List[str], dry_run: bool = False) -> None:
    if not labels:
        return
    existing = set(map(str.lower, get_existing_labels(repo)))
    for label in labels:
        if label.lower() in existing:
            continue
        if dry_run:
            print(f"DRY-RUN: would create label '{label}' in {repo}")
            continue
        code, out, err = run(["gh", "label", "create", label, "-R", repo])
        if code != 0:
            # If creation fails, report but continue
            print(f"WARN: failed to create label '{label}': {err}", file=sys.stderr)


def get_milestones_map(owner: str, repo_name: str) -> Dict[str, int]:
    # Use gh api with pagination to list all milestones
    code, out, err = run(["gh", "api", "--paginate", f"repos/{owner}/{repo_name}/milestones"])
    if code != 0:
        raise RuntimeError(f"Failed to list milestones: {err}")
    try:
        items = json.loads(out)
    except json.JSONDecodeError:
        # When --paginate is used, output may be concatenated arrays. Fallback to merging lines.
        items = []
        for line in out.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                part = json.loads(line)
                if isinstance(part, list):
                    items.extend(part)
            except json.JSONDecodeError:
                pass
    mapping: Dict[str, int] = {}
    if isinstance(items, list):
        for m in items:
            if isinstance(m, dict) and "title" in m and "number" in m:
                mapping[str(m["title"]).lower()] = int(m["number"])  # milestone number
    return mapping


def ensure_milestone(owner: str, repo_name: str, title: str, dry_run: bool = False) -> int:
    ms_map = get_milestones_map(owner, repo_name)
    key = title.lower()
    if key in ms_map:
        return ms_map[key]
    if dry_run:
        print(f"DRY-RUN: would create milestone '{title}' in {owner}/{repo_name}")
        # Return a dummy placeholder; caller should guard when dry-run
        return -1
    code, out, err = run([
        "gh", "api", "-X", "POST", f"repos/{owner}/{repo_name}/milestones", "-f", f"title={title}"
    ])
    if code != 0:
        raise RuntimeError(f"Failed to create milestone '{title}': {err}")
    data = json.loads(out)
    return int(data.get("number"))


def create_issue_via_api(owner: str, repo_name: str, row: IssueRow, milestone_number: Optional[int], dry_run: bool = False) -> Tuple[int, str]:
    endpoint = f"repos/{owner}/{repo_name}/issues"
    payload: Dict[str, object] = {"title": row.title}
    if row.body:
        payload["body"] = row.body
    if row.labels:
        payload["labels"] = row.labels
    if row.assignees:
        payload["assignees"] = row.assignees
    if milestone_number and milestone_number > 0:
        payload["milestone"] = milestone_number

    if dry_run:
        print("DRY-RUN: would POST JSON to", endpoint)
        print(json.dumps(payload, indent=2))
        return -1, "DRY-RUN"

    args = ["gh", "api", "-X", "POST", endpoint, "--input", "-"]
    body_bytes = json.dumps(payload).encode("utf-8")
    code, out, err = run(args, input_bytes=body_bytes)
    if code != 0:
        raise RuntimeError(f"Issue creation failed for '{row.title}': {err}\nRequest: {json.dumps(payload)}")
    data = json.loads(out)
    return int(data["number"]), str(data["html_url"])  # type: ignore


def create_issue_with_retry(owner: str, repo_name: str, row: IssueRow, milestone_number: Optional[int], dry_run: bool, retries: int, retry_base_delay: float) -> Tuple[int, str]:
    """Attempt to create an issue with exponential backoff retry for transient errors."""
    attempt = 0
    while True:
        attempt += 1
        try:
            return create_issue_via_api(owner, repo_name, row, milestone_number, dry_run=dry_run)
        except Exception as e:  # noqa: BLE001
            transient = any(token in str(e).lower() for token in [
                "timeout", "timed out", "rate limit", "502", "503", "500", "network", "temporarily unavailable"
            ])
            if attempt >= retries or not transient or dry_run:
                raise
            sleep_for = retry_base_delay * (2 ** (attempt - 1))
            print(f"Retry {attempt}/{retries} after error: {e}. Sleeping {sleep_for:.1f}s...", file=sys.stderr)
            time.sleep(sleep_for)


def add_issue_to_project_v2(project_owner: str, project_number: int, issue_url: str, dry_run: bool = False) -> None:
    args = [
        "gh", "project", "item-add", str(project_number), "--owner", project_owner, "--url", issue_url,
    ]
    if dry_run:
        print("DRY-RUN: would run:")
        print(" ", " ".join(sh_quote(a) for a in args))
        return
    code, out, err = run(args)
    if code != 0:
        raise RuntimeError(f"Failed to add issue to project {project_owner}/{project_number}: {err}")


def close_issue(issue_url_or_number: str, repo: str, reason: Optional[str], dry_run: bool = False) -> None:
    args = ["gh", "issue", "close", issue_url_or_number, "-R", repo]
    if reason:
        args += ["--reason", reason]
    if dry_run:
        print("DRY-RUN: would run:")
        print(" ", " ".join(sh_quote(a) for a in args))
        return
    code, out, err = run(args)
    if code != 0:
        raise RuntimeError(f"Failed to close issue {issue_url_or_number}: {err}")


def sh_quote(s: str) -> str:
    if not s:
        return "''"
    if all(c.isalnum() or c in "/._-=:,[]{}" for c in s):
        return s
    return "'" + s.replace("'", "'\''") + "'"


# -------------------------- Main flow --------------------------

def read_csv(path: str) -> List[IssueRow]:
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        rows: List[IssueRow] = []
        for i, raw in enumerate(reader, start=2):  # header at line 1
            if not raw:
                continue
            # Normalize keys (case-insensitive)
            norm = { (k or "").strip().lower(): (v or "").strip() for k, v in raw.items() }
            title = norm.get("title", "")
            if not title:
                print(f"Skipping row {i}: missing title")
                continue
            body = norm.get("body", "")
            labels = split_list(norm.get("labels", ""))
            assignees = split_list(norm.get("assignees", ""))
            milestone = norm.get("milestone") or None
            project_owner = norm.get("project_owner") or None
            pn = norm.get("project_number") or None
            project_number = int(pn) if pn and pn.isdigit() else None
            state = (norm.get("state") or "open").lower()
            close_reason = norm.get("close_reason") or None
            rows.append(IssueRow(title=title, body=body, labels=labels, assignees=assignees, milestone=milestone, project_owner=project_owner, project_number=project_number, state=state, close_reason=close_reason))
        return rows


def main() -> int:
    ap = argparse.ArgumentParser(description="Import GitHub issues from CSV via gh CLI")
    ap.add_argument("--csv", required=True, help="Path to CSV file")
    ap.add_argument("--repo", help="Target repository in owner/name format; defaults to current gh repo context")
    ap.add_argument("--delay", type=float, default=0.0, help="Seconds to sleep between issue creations")
    ap.add_argument("--dry-run", action="store_true", help="Do not create or modify anything; print actions")
    ap.add_argument("--no-label-create", action="store_true", help="Do not attempt to auto-create missing labels")
    ap.add_argument("--no-milestone-create", action="store_true", help="Do not attempt to auto-create missing milestones")
    ap.add_argument("--retries", type=int, default=3, help="Issue creation retry attempts (default 3)")
    ap.add_argument("--retry-base-delay", type=float, default=1.0, help="Base seconds for exponential backoff (default 1.0)")
    ap.add_argument("--no-progress", action="store_true", help="Disable progress bar output")
    args = ap.parse_args()

    csv_path = args.csv
    if not os.path.exists(csv_path):
        print(f"CSV not found: {csv_path}", file=sys.stderr)
        return 2

    repo_full = ensure_repo_name_with_owner(args.repo)
    owner, repo_name = repo_full.split("/", 1)

    rows = read_csv(csv_path)
    if not rows:
        print("No rows found in CSV.")
        return 0

    # Aggregate label set to pre-create if allowed
    if not args.no_label_create:
        all_labels = sorted({lbl for r in rows for lbl in r.labels})
        ensure_labels(repo_full, all_labels, dry_run=args.dry_run)

    # Prepare milestones mapping (read once)
    milestone_cache = None  # type: Optional[Dict[str, int]]

    total = len(rows)
    created: List[Tuple[int, str, str]] = []  # (row_index, issue_number, issue_url)
    errors: List[str] = []

    def render_progress(done: int, current_title: Optional[str] = None):
        if args.no_progress:
            return
        bar_width = 40
        filled = int(math.floor((done / total) * bar_width)) if total else 0
        bar = "#" * filled + "-" * (bar_width - filled)
        pct = (done / total * 100) if total else 100.0
        line = f"[{bar}] {done}/{total} ({pct:5.1f}%)"
        if current_title:
            trimmed = (current_title[:50] + "…") if len(current_title) > 51 else current_title
            line += f" {trimmed}"
        sys.stdout.write("\r" + line)
        sys.stdout.flush()

    if not args.no_progress:
        print(f"Importing {total} issues...")
        render_progress(0)

    for idx, row in enumerate(rows, start=1):
        if not args.no_progress:
            render_progress(idx - 1, row.title)
        try:
            milestone_number: Optional[int] = None
            if row.milestone:
                if milestone_cache is None:
                    milestone_cache = get_milestones_map(owner, repo_name)
                key = row.milestone.lower()
                if key not in (milestone_cache or {}):
                    if args.no_milestone_create:
                        print(f"INFO: milestone '{row.milestone}' missing; skipping milestone for '{row.title}'")
                    else:
                        msn = ensure_milestone(owner, repo_name, row.milestone, dry_run=args.dry_run)
                        if msn > 0:
                            milestone_cache[key] = msn
                milestone_number = (milestone_cache or {}).get(key)

            number, url = create_issue_with_retry(
                owner,
                repo_name,
                row,
                milestone_number,
                dry_run=args.dry_run,
                retries=max(1, args.retries),
                retry_base_delay=max(0.1, args.retry_base_delay),
            )
            if args.dry_run:
                # In dry-run, fabricate a URL for subsequent messages
                url = f"https://github.com/{owner}/{repo_name}/issues/<new>"

            # Projects v2
            if row.project_owner and row.project_number:
                add_issue_to_project_v2(row.project_owner, row.project_number, url, dry_run=args.dry_run)

            # State/close handling
            if row.state == "closed":
                close_issue(url, repo_full, reason=row.close_reason, dry_run=args.dry_run)

            created.append((idx, str(number), url))
            if args.delay > 0 and not args.dry_run:
                time.sleep(args.delay)
        except Exception as e:
            msg = f"Row {idx} ('{row.title}') failed: {e}"
            print(f"ERROR: {msg}", file=sys.stderr)
            errors.append(msg)
        if not args.no_progress:
            render_progress(idx)

    if not args.no_progress:
        # Finish progress line with newline
        sys.stdout.write("\n")
        sys.stdout.flush()

    # Summary
    print("\nSummary:")
    print(f"  Created: {len(created)} issues")
    for _, num, url in created:
        print(f"   - #{num} -> {url}")
    if errors:
        print(f"  Errors: {len(errors)}", file=sys.stderr)
        for e in errors:
            print(f"   - {e}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("Aborted.")
        sys.exit(130)
