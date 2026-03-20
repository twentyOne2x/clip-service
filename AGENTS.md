# AGENTS Map

This file is a short navigation map for agent workflows in this repo.

## Product Surface
- `main.py`: FastAPI app, job lifecycle, and clip endpoint behavior.
- `tests/`: service-level tests.
- `Dockerfile`: container runtime packaging.

## Knowledge Base
- `docs/index.md`: entrypoint to architecture, beliefs, and plans.
- `docs/core-beliefs.md`: engineering and product principles.
- `ARCHITECTURE.md`: service boundaries and runtime flow.
- `docs/plans/`: active/completed plans and debt tracker.

## Answer Then Act
- Answer the user's question directly.
- If the truthful answer to a sufficiency, completeness, or quality question is "no" and the concrete fix is inferable from the repo and thread context, implement the fix instead of stopping at analysis.
- If the truthful answer is "yes", report that and stop.
- If a real blocker remains, report the blocker clearly.

## Verification
- Run `python3 scripts/knowledge_check.py` before PR creation.
