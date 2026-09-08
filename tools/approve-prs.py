#!/usr/bin/env python3
"""Bulk-approve GitHub pull requests via the gh CLI.

Usage:
    approve-prs.py 12 34 owner/repo#56 https://github.com/owner/repo/pull/78
    approve-prs.py --repo owner/repo --file prs.txt --body "LGTM" --yes
    approve-prs.py --repo owner/repo --search "is:open author:alice label:deps"
    cat prs.txt | approve-prs.py --repo owner/repo
"""

import argparse
import json
import re
import subprocess
import sys

URL_RE = re.compile(r"github\.com/([^/]+/[^/]+)/pull/(\d+)")
SHORT_RE = re.compile(r"^([\w.-]+/[\w.-]+)#(\d+)$")
NUM_RE = re.compile(r"^#?(\d+)$")


def parse_ref(token, default_repo):
    """Return (repo, number) for one PR reference, or raise ValueError."""
    token = token.strip().rstrip(",;")
    m = URL_RE.search(token) or SHORT_RE.match(token)
    if m:
        return m.group(1), int(m.group(2))
    m = NUM_RE.match(token)
    if m:
        if not default_repo:
            raise ValueError(f"{token!r} needs --repo")
        return default_repo, int(m.group(1))
    raise ValueError(f"unrecognised PR reference: {token!r}")


def gh(*args, check=True):
    return subprocess.run(
        ["gh", *args], capture_output=True, text=True, check=check
    )


def search_prs(repo, query):
    print(f"searching {repo}: {query} ...", file=sys.stderr, flush=True)
    out = gh(
        "pr", "list", "--repo", repo, "--search", query,
        "--state", "open", "--limit", "100", "--json", "number,title",
    ).stdout
    return [(repo, item["number"]) for item in json.loads(out)]


def collect_refs(args):
    tokens = list(args.refs)
    if args.file:
        with open(args.file) as fh:
            tokens += fh.read().split()
    elif not tokens and not args.search:
        if sys.stdin.isatty():
            print("Paste PR links, then Ctrl-D:", file=sys.stderr)
        tokens += sys.stdin.read().split()

    refs, skipped = [], []
    for token in tokens:
        try:
            refs.append(parse_ref(token, args.repo))
        except ValueError as exc:
            skipped.append(str(exc))
    if args.search:
        if not args.repo:
            sys.exit("error: --search needs --repo")
        refs += search_prs(args.repo, args.search)
    for line in skipped:
        print(f"skip: {line}", file=sys.stderr)
    # dedup, keep order
    return list(dict.fromkeys(refs))


def approve(repo, number, body):
    cmd = ["pr", "review", str(number), "--repo", repo, "--approve"]
    if body:
        cmd += ["--body", body]
    result = gh(*cmd, check=False)
    ok = result.returncode == 0
    detail = (result.stderr or result.stdout).strip().splitlines()
    return ok, detail[-1] if detail else ""


def self_check():
    assert parse_ref("42", "o/r") == ("o/r", 42)
    assert parse_ref("#42", "o/r") == ("o/r", 42)
    assert parse_ref("a/b#7", None) == ("a/b", 7)
    assert parse_ref("https://github.com/a/b/pull/9/files", None) == ("a/b", 9)
    assert parse_ref("https://github.com/o/r-x/pull/8102/changes", None) == ("o/r-x", 8102)
    assert parse_ref("https://github.com/a/b/pull/3,", None) == ("a/b", 3)
    for bad, repo in [("42", None), ("(edited)", "o/r"), ("", "o/r")]:
        try:
            parse_ref(bad, repo)
        except ValueError:
            continue
        raise AssertionError(f"expected failure for {bad!r}")
    print("self-check ok")


def main():
    parser = argparse.ArgumentParser(description="Bulk-approve GitHub PRs.")
    parser.add_argument("refs", nargs="*", help="PR numbers, owner/repo#N, or URLs")
    parser.add_argument("--repo", help="default repo for bare numbers (owner/repo)")
    parser.add_argument("--file", help="file with PR references (whitespace separated)")
    parser.add_argument("--search", help="gh pr list --search query (needs --repo)")
    parser.add_argument("--body", help="review comment")
    parser.add_argument("-n", "--dry-run", action="store_true")
    parser.add_argument("-y", "--yes", action="store_true", help="skip confirmation")
    parser.add_argument("--self-check", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()

    if args.self_check:
        return self_check()

    refs = collect_refs(args)
    if not refs:
        sys.exit("nothing to approve")

    for repo, number in refs:
        print(f"  {repo}#{number}")
    print(f"{len(refs)} PR(s) to approve.")

    if args.dry_run:
        return
    if not args.yes:
        if input("Approve all of these? [y/N] ").strip().lower() not in ("y", "yes"):
            sys.exit("aborted")

    failed = 0
    for repo, number in refs:
        ok, detail = approve(repo, number, args.body)
        print(f"{'OK  ' if ok else 'FAIL'} {repo}#{number} {'' if ok else detail}")
        failed += not ok
    if failed:
        sys.exit(f"{failed} of {len(refs)} failed")


if __name__ == "__main__":
    main()
