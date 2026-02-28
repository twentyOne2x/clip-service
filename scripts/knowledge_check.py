#!/usr/bin/env python3
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
REQUIRED_FILES = [
    ROOT / "AGENTS.md",
    ROOT / "docs/index.md",
    ROOT / "docs/core-beliefs.md",
    ROOT / "ARCHITECTURE.md",
    ROOT / "docs/plans/plan-template.md",
    ROOT / "docs/plans/tech-debt-tracker.md",
    ROOT / ".github/workflows/knowledge-base.yml",
]
REQUIRED_DIRS = [
    ROOT / "docs/plans/active",
    ROOT / "docs/plans/completed",
]


def main() -> int:
    missing = []
    for f in REQUIRED_FILES:
        if not f.is_file():
            missing.append(str(f.relative_to(ROOT)))
    for d in REQUIRED_DIRS:
        if not d.is_dir():
            missing.append(str(d.relative_to(ROOT)) + "/")

    agents = ROOT / "AGENTS.md"
    if agents.is_file():
        line_count = len(agents.read_text(encoding="utf-8").splitlines())
        if line_count > 120:
            missing.append("AGENTS.md exceeds 120 lines; keep it a short map")

    idx = ROOT / "docs/index.md"
    if idx.is_file():
        idx_text = idx.read_text(encoding="utf-8")
        required_links = ["core-beliefs", "ARCHITECTURE", "plan-template", "tech-debt-tracker"]
        for marker in required_links:
            if marker not in idx_text:
                missing.append(f"docs/index.md missing reference marker: {marker}")

    if missing:
        print("Knowledge check failed:")
        for item in missing:
            print(f"- {item}")
        return 1

    print("Knowledge check passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
