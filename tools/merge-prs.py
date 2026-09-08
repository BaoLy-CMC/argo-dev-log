#!/usr/bin/env python3
"""Bulk-merge the mergeable GitHub pull requests via the gh CLI.

Reuses the PR-reference parsing from approve-prs.py. PRs that are not
mergeable (conflicts, blocked reviews, failing checks, draft) are skipped
and reported, never force-merged.

Usage:
    merge-prs.py 12 34 owner/repo#56 https://github.com/owner/repo/pull/78
    merge-prs.py --repo owner/repo --search "is:open author:alice" --yes
    merge-prs.py --repo owner/repo --file prs.txt --rebase --no-delete-branch
    merge-prs.py --repo owner/repo --search "label:deps" --allow UNSTABLE
"""

import argparse
import importlib.util
import json
import pathlib
import sys
import time

_spec = importlib.util.spec_from_file_location(
    "approve_prs", pathlib.Path(__file__).with_name("approve-prs.py")
)
approve_prs = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(approve_prs)

gh = approve_prs.gh
collect_refs = approve_prs.collect_refs

DEFAULT_ALLOWED = {"CLEAN", "HAS_HOOKS"}
STATUS_HINT = {
    "DIRTY": "merge conflicts",
    "BLOCKED": "blocked (missing approval or required check)",
    "BEHIND": "behind base branch (use --update-branch)",
    "UNSTABLE": "checks failing or pending",
    "DRAFT": "draft PR",
    "UNKNOWN": "mergeability not computed yet",
}


def pr_state(repo, number):
    result = gh(
        "pr", "view", str(number), "--repo", repo,
        "--json", "number,state,isDraft,mergeable,mergeStateStatus,title",
        check=False,
    )
    if result.returncode != 0:
        return None, (result.stderr or result.stdout).strip().splitlines()[-1:] or [""]
    return json.loads(result.stdout), None


def classify(pr, allowed):
    if pr["state"] != "OPEN":
        return False, f"state {pr['state'].lower()}"
    if pr["isDraft"]:
        return False, STATUS_HINT["DRAFT"]
    if pr["mergeable"] == "CONFLICTING":
        return False, STATUS_HINT["DIRTY"]
    status = pr["mergeStateStatus"]
    if status in allowed:
        return True, status
    return False, STATUS_HINT.get(status, status)


def note(text):
    print(text, file=sys.stderr, flush=True)


def resolve(repo, number, allowed, retries=2):
    """Fetch state, retrying while GitHub still reports UNKNOWN mergeability."""
    for attempt in range(retries + 1):
        pr, err = pr_state(repo, number)
        if err:
            return None, err[0]
        if pr["mergeable"] != "UNKNOWN" or attempt == retries:
            return pr, None
        note(f"        mergeability unknown, retry {attempt + 1}/{retries} in 2s")
        time.sleep(2)


def merge(repo, number, args):
    cmd = ["pr", "merge", str(number), "--repo", repo, f"--{args.method}"]
    if args.delete_branch:
        cmd.append("--delete-branch")
    if args.update_branch:
        cmd.append("--auto")
    if args.admin:
        cmd.append("--admin")
    result = gh(*cmd, check=False)
    detail = (result.stderr or result.stdout).strip().splitlines()
    return result.returncode == 0, detail[-1] if detail else ""


def self_check():
    allowed = DEFAULT_ALLOWED
    base = {"state": "OPEN", "isDraft": False, "mergeable": "MERGEABLE"}
    assert classify({**base, "mergeStateStatus": "CLEAN"}, allowed)[0]
    assert classify({**base, "mergeStateStatus": "HAS_HOOKS"}, allowed)[0]
    assert not classify({**base, "mergeStateStatus": "UNSTABLE"}, allowed)[0]
    assert classify({**base, "mergeStateStatus": "UNSTABLE"}, allowed | {"UNSTABLE"})[0]
    assert not classify({**base, "mergeStateStatus": "BLOCKED"}, allowed)[0]
    assert not classify({**base, "mergeable": "CONFLICTING", "mergeStateStatus": "DIRTY"}, allowed)[0]
    assert not classify({**base, "isDraft": True, "mergeStateStatus": "CLEAN"}, allowed)[0]
    assert not classify({**base, "state": "MERGED", "mergeStateStatus": "CLEAN"}, allowed)[0]
    print("self-check ok")


def main():
    parser = argparse.ArgumentParser(description="Bulk-merge mergeable GitHub PRs.")
    parser.add_argument("refs", nargs="*", help="PR numbers, owner/repo#N, or URLs")
    parser.add_argument("--repo", help="default repo for bare numbers (owner/repo)")
    parser.add_argument("--file", help="file with PR references (whitespace separated)")
    parser.add_argument("--search", help="gh pr list --search query (needs --repo)")
    method = parser.add_mutually_exclusive_group()
    method.add_argument("--squash", dest="method", action="store_const", const="squash")
    method.add_argument("--merge", dest="method", action="store_const", const="merge")
    method.add_argument("--rebase", dest="method", action="store_const", const="rebase")
    parser.add_argument("--allow", action="append", default=[], metavar="STATUS",
                        help="extra mergeStateStatus to accept (e.g. UNSTABLE, BEHIND)")
    parser.add_argument("--no-delete-branch", dest="delete_branch",
                        action="store_false", default=True)
    parser.add_argument("--update-branch", action="store_true",
                        help="enable auto-merge so GitHub merges once checks pass")
    parser.add_argument("--admin", action="store_true",
                        help="bypass branch protection (needs admin rights)")
    parser.add_argument("-n", "--dry-run", action="store_true")
    parser.add_argument("-y", "--yes", action="store_true", help="skip confirmation")
    parser.add_argument("--self-check", action="store_true", help=argparse.SUPPRESS)
    parser.set_defaults(method="squash")
    args = parser.parse_args()

    if args.self_check:
        return self_check()

    refs = collect_refs(args)
    if not refs:
        sys.exit("nothing to merge")

    allowed = DEFAULT_ALLOWED | {s.upper() for s in args.allow}
    total = len(refs)
    note(f"Checking {total} PR(s) with gh...")
    ready, blocked, errors = [], [], []
    for index, (repo, number) in enumerate(refs, 1):
        prefix = f"[{index}/{total}]"
        note(f"{prefix} {repo}#{number} ...")
        pr, err = resolve(repo, number, allowed)
        if err:
            errors.append((repo, number, err))
            note(f"{prefix} ERROR   {repo}#{number} {err}")
            continue
        ok, reason = classify(pr, allowed)
        (ready if ok else blocked).append((repo, number, pr["title"], reason))
        label = "READY" if ok else "SKIP "
        print(f"{prefix} {label}   {repo}#{number} [{reason}] {pr['title']}", flush=True)

    if not ready:
        sys.exit(f"no mergeable PR ({len(blocked)} skipped, {len(errors)} error)")
    print(f"{len(ready)} mergeable of {total} PR(s), method={args.method}.")

    if args.dry_run:
        return
    if not args.yes:
        if input("Merge all ready PRs? [y/N] ").strip().lower() not in ("y", "yes"):
            sys.exit("aborted")

    failed = 0
    for index, (repo, number, _title, _reason) in enumerate(ready, 1):
        prefix = f"[{index}/{len(ready)}]"
        note(f"{prefix} merging {repo}#{number} ...")
        ok, detail = merge(repo, number, args)
        print(f"{prefix} {'OK  ' if ok else 'FAIL'} {repo}#{number} {'' if ok else detail}", flush=True)
        failed += not ok
    if failed or errors:
        sys.exit(f"{failed} merge failure(s), {len(errors)} lookup error(s)")


if __name__ == "__main__":
    main()
