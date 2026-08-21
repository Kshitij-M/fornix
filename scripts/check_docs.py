#!/usr/bin/env python3
"""Validate the repository's public Markdown documentation contract."""

from __future__ import annotations

import re
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
MARKDOWN_FILES = [
    *sorted(ROOT.glob("*.md")),
    *sorted((ROOT / "docs").glob("*.md")),
    *sorted((ROOT / ".github").rglob("*.md")),
]
LINK_RE = re.compile(r"\[[^\]]+\]\(([^)\n]+)\)")
PRIVATE_PATH_RE = re.compile(r"/(?:Users|home)/[A-Za-z0-9._-]+/")


def local_target(source: Path, raw_target: str) -> Path | None:
    target = raw_target.strip()
    if target.startswith("<") and ">" in target:
        target = target[1 : target.index(">")]
    else:
        target = target.split(maxsplit=1)[0]
    if not target or target.startswith("#"):
        return None
    if "://" in target or target.startswith("mailto:"):
        return None
    target = target.split("#", maxsplit=1)[0]
    if not target:
        return None
    return (source.parent / target).resolve()


def check_file(path: Path) -> list[str]:
    errors: list[str] = []
    text = path.read_text(encoding="utf-8")
    lines = text.splitlines()
    nonempty = next((line for line in lines if line.strip()), "")
    if not nonempty.startswith("# "):
        errors.append("first non-empty line must be a level-one heading")
    if path.parent == ROOT / "docs" and not any(
        line.startswith("Status:") for line in lines[:12]
    ):
        errors.append("docs files must declare Status in their first 12 lines")
    if PRIVATE_PATH_RE.search(text):
        errors.append("contains a developer-specific /Users or /home path")
    for match in LINK_RE.finditer(text):
        target = local_target(path, match.group(1))
        if target is not None and not target.exists():
            errors.append(f"broken local link: {match.group(1).strip()}")
    return errors


def main() -> int:
    failures: list[str] = []
    for path in MARKDOWN_FILES:
        if not path.exists():
            failures.append(f"missing documentation file: {path.relative_to(ROOT)}")
            continue
        for error in check_file(path):
            failures.append(f"{path.relative_to(ROOT)}: {error}")
    if failures:
        print("documentation check failed:", file=sys.stderr)
        for failure in failures:
            print(f"- {failure}", file=sys.stderr)
        return 1
    print(f"documentation check passed ({len(MARKDOWN_FILES)} Markdown files)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
