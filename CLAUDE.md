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
