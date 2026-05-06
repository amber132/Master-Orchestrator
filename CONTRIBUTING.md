# Contributing to Master Orchestrator

Thanks for your interest in contributing! This document provides guidelines and steps for contributing.

## Getting Started

```bash
# Fork and clone the repo
git clone https://github.com/<your-username>/Master-Orchestrator.git
cd Master-Orchestrator

# Set up development environment
python -m venv .venv
source .venv/bin/activate  # or .venv\Scripts\activate on Windows
pip install -e ".[dev]"

# Run tests to verify setup
python -m pytest -q
```

## Development Workflow

1. Create a branch from `main`:
   ```bash
   git checkout -b feat/your-feature
   ```

2. Make your changes, following the existing code style

3. Add or update tests for your changes

4. Run the test suite:
   ```bash
   python -m pytest -q
   ```

5. Commit with a descriptive message:
   ```bash
   git commit -m "feat: add support for custom validation rules"
   ```

6. Push and open a Pull Request

## Commit Convention

We follow [Conventional Commits](https://www.conventionalcommits.org/):

- `feat:` — new feature
- `fix:` — bug fix
- `docs:` — documentation changes
- `refactor:` — code refactoring
- `test:` — adding or updating tests
- `chore:` — maintenance tasks

## Code Style

- Python 3.11+ with type hints
- Follow existing patterns in the codebase
- Keep functions focused and modules cohesive
- Docstrings for public APIs (Google style)

## Reporting Issues

- Use GitHub Issues for bug reports and feature requests
- Include reproduction steps, expected vs actual behavior
- Attach relevant logs or error messages

## Areas Where Help Is Needed

- **Provider integrations** — support for additional AI coding tools
- **Documentation** — tutorials, examples, translations
- **Testing** — edge cases, integration tests
- **Simple mode** — new validation checkers, bulk operation patterns

## Questions?

Open a Discussion on GitHub or comment on an existing issue.
