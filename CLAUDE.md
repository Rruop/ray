# Ray

Ray is an open-source unified framework for scaling AI and Python applications.
Version 2.52.1 | Python (primary), C++, Java | Bazel build system

## Core Commands

```bash
# Development install
pip install -e "python[all]"
SKIP_BAZEL_BUILD=1 pip install -e "python[all]"  # Skip Bazel build

# Run tests
pytest python/ray/tests/test_xxx.py -v
pytest python/ray/serve/tests/ -v
pytest python/ray/data/tests/ -v

# Lint
./ci/lint/lint.sh pre_commit
pre-commit run --all-files

# Build
bazel build //:gen_ray_pkg
```

## Project Structure

```
ray/
├── python/ray/           # Python core package
│   ├── serve/           # Model serving (Ray Serve)
│   ├── data/            # Data pipeline (Ray Data)
│   ├── train/           # Distributed training (Ray Train)
│   ├── tune/            # Hyperparameter tuning (Ray Tune)
│   ├── llm/             # LLM utilities
│   ├── dag/             # DAG API
│   ├── dashboard/       # Web dashboard
│   ├── autoscaler/      # Cluster autoscaling
│   └── _private/        # Internal implementation
├── rllib/               # Reinforcement learning library
├── src/ray/             # C++ core (raylet, GCS, object store)
├── java/                # Java bindings
├── cpp/                 # C++ API
├── ci/                  # CI scripts and lint tools
└── bazel/               # Bazel build configs
```

## Architecture Overview

```
┌─────────────────────────────────────────────────────────┐
│                     Ray Libraries                        │
│  Ray Serve │ Ray Data │ Ray Train │ Ray Tune │ RLlib    │
├─────────────────────────────────────────────────────────┤
│                     Ray Core                             │
│  Tasks │ Actors │ Objects │ Placement Groups            │
├─────────────────────────────────────────────────────────┤
│                  Cluster Layer                           │
│  Raylet │ GCS │ Object Store │ Autoscaler               │
└─────────────────────────────────────────────────────────┘
```

## Code Style

- **Python**: ruff + black (line-length=88)
- **C++**: clang-format, cpplint
- **Bazel**: buildifier
- **Pre-commit**: `pre-commit run --all-files`

## Key Files

- `python/ray/__init__.py` - Main entry point
- `python/ray/_raylet.pyx` - Cython bindings to C++ core
- `src/ray/raylet/raylet.cc` - Raylet main process
- `src/ray/gcs/gcs_server/gcs_server.cc` - GCS server
- `.pre-commit-config.yaml` - Lint configuration

## CI/CD

- **Primary CI**: Buildkite
- **Test categories**: unit, integration, release tests
- **Lint checks**: ruff, black, mypy, clang-format, buildifier, shellcheck

---

## Behavioral Guidelines (Karpathy)

Behavioral guidelines to reduce common LLM coding mistakes.

**Tradeoff:** These guidelines bias toward caution over speed. For trivial tasks, use judgment.

### 1. Think Before Coding

**Don't assume. Don't hide confusion. Surface tradeoffs.**

Before implementing:
- State your assumptions explicitly. If uncertain, ask.
- If multiple interpretations exist, present them - don't pick silently.
- If a simpler approach exists, say so. Push back when warranted.
- If something is unclear, stop. Name what's confusing. Ask.

### 2. Simplicity First

**Minimum code that solves the problem. Nothing speculative.**

- No features beyond what was asked.
- No abstractions for single-use code.
- No "flexibility" or "configurability" that wasn't requested.
- No error handling for impossible scenarios.
- If you write 200 lines and it could be 50, rewrite it.

Ask yourself: "Would a senior engineer say this is overcomplicated?" If yes, simplify.

### 3. Surgical Changes

**Touch only what you must. Clean up only your own mess.**

When editing existing code:
- Don't "improve" adjacent code, comments, or formatting.
- Don't refactor things that aren't broken.
- Match existing style, even if you'd do it differently.
- If you notice unrelated dead code, mention it - don't delete it.

When your changes create orphans:
- Remove imports/variables/functions that YOUR changes made unused.
- Don't remove pre-existing dead code unless asked.

The test: Every changed line should trace directly to the user's request.

### 4. Goal-Driven Execution

**Define success criteria. Loop until verified.**

Transform tasks into verifiable goals:
- "Add validation" → "Write tests for invalid inputs, then make them pass"
- "Fix the bug" → "Write a test that reproduces it, then make it pass"
- "Refactor X" → "Ensure tests pass before and after"

For multi-step tasks, state a brief plan:
```
1. [Step] → verify: [check]
2. [Step] → verify: [check]
3. [Step] → verify: [check]
```

Strong success criteria let you loop independently. Weak criteria ("make it work") require constant clarification.

---

**These guidelines are working if:** fewer unnecessary changes in diffs, fewer rewrites due to overcomplication, and clarifying questions come before implementation rather than after mistakes.