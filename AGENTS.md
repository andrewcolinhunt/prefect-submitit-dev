# AGENTS.md

Project conventions for contributors (human and AI).

---

## Environment

| Setting     | Value                     |
| ----------- | ------------------------- |
| Environment | Pixi (`~/.pixi/bin/pixi`) |
| Formatting  | Ruff                      |
| Testing     | Pytest                    |

IMPORTANT: Always use the full pixi path (`~/.pixi/bin/pixi`) when running
commands. The short `pixi` form is for user-facing docs only.

---

## Commands

```bash
~/.pixi/bin/pixi install --locked
~/.pixi/bin/pixi run --locked -e dev test
~/.pixi/bin/pixi run --locked -e dev test-sbatch
~/.pixi/bin/pixi run --locked -e dev test-sbatch-docker
~/.pixi/bin/pixi run --locked -e dev test-srun
~/.pixi/bin/pixi run --locked -e dev test-srun-docker
~/.pixi/bin/pixi run --locked -e dev fmt
~/.pixi/bin/pixi run --locked prefect-start
~/.pixi/bin/pixi run --locked prefect-stop
~/.pixi/bin/pixi run --locked install-kernel
~/.pixi/bin/pixi run --locked slurm-build
~/.pixi/bin/pixi run --locked slurm-up
~/.pixi/bin/pixi run --locked slurm-down
~/.pixi/bin/pixi run --locked slurm-shell
~/.pixi/bin/pixi run --locked python script.py
```

### Pixi lock discipline

Use `--locked` for routine installs and tasks so Pixi stops instead of silently
rewriting `pixi.lock`. Run an unlocked Pixi command only when intentionally
changing dependencies, and commit `pyproject.toml` and `pixi.lock` together.

If `pixi.lock` becomes dirty unexpectedly, inspect the diff. When the only
changes are the editable project's Git-derived `version` and `sha256`, and no
dependency change was intended, restore `pixi.lock`. Do not discard it when
`pyproject.toml` or dependencies were intentionally changed.

---

## Code Style

- **DRY, YAGNI, KISS** — no premature abstractions
- **No backwards-compat shims** — when removing/renaming, delete completely
- **Fail fast** — validate inputs early, raise clear exceptions
- **Type hints** on all function signatures
- **Comments for "why"**, not "what"
- **Functions < 30 lines** ideally
- **Specific exceptions** — no bare `except:`
- **Google-style docstrings:**

```python
def example(param1: str, param2: int = 0) -> bool:
    """Short description.

    Args:
        param1: Description.
        param2: Description. Defaults to 0.

    Returns:
        Description.

    Raises:
        ValueError: When param1 is empty.
    """
```

---

## Testing

Tests mirror source structure: `tests/`

- Files: `test_<module>.py`
- Functions: `test_<function>_<scenario>`
- Cover: happy path, edge cases, error conditions
- `@pytest.mark.slurm` for tests that submit real SLURM jobs
- `@pytest.mark.slurm_gpu` for tests requiring a GPU partition
- Integration tests in `tests/integration/`

---

## Architecture

```
src/prefect_submitit/          # Prefect TaskRunner for SLURM via submitit
├── __init__.py                # Public API exports
├── runner.py                  # SlurmTaskRunner (main entry point)
├── submission.py              # SLURM job submission logic
├── executors.py               # Submitit executor wrappers
├── constants.py               # Default values and env var names
├── utils.py                   # Utility functions
├── futures/                   # Prefect future implementations
│   ├── base.py                # Base future class
│   ├── array.py               # Job array futures
│   └── batched.py             # Batched execution futures
└── server/                    # Prefect server lifecycle (CLI)
    ├── cli.py                 # `prefect-server` entry point
    ├── config.py              # Server configuration
    ├── discovery.py           # Server discovery file management
    ├── postgres.py            # PostgreSQL init and management
    └── prefect_proc.py        # Prefect server process control

examples/                      # Demo notebooks
tests/                         # Test suite
```

---

## Git Conventions

### Branch Naming

```
feat/[name]      fix/[name]       refactor/[name]
docs/[name]      test/[name]      chore/[name]
```

### Commit Format

```
type: Brief description

```

Types: `feat`, `fix`, `refactor`, `docs`, `test`, `chore`, `perf`, `style`

### PR Validation Order

1. `~/.pixi/bin/pixi run --locked -e dev fmt`
2. `~/.pixi/bin/pixi run --locked -e dev test`

---

## Pre-PR Checklist

- [ ] Code: no debug prints, no commented-out code
- [ ] Tests pass (`~/.pixi/bin/pixi run --locked -e dev test`), new code has
      tests
- [ ] Formatted and linted (`~/.pixi/bin/pixi run --locked -e dev fmt`)
- [ ] Commits are atomic with proper messages
- [ ] Self-reviewed all changes

---

## Personal Overrides

Personal agent settings are tool-specific and must remain gitignored. Claude
Code users may use `CLAUDE.local.md`.
