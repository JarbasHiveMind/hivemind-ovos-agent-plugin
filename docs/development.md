# Development

## Local setup

```bash
git clone https://github.com/JarbasHiveMind/hivemind-ovos-agent-plugin
cd hivemind-ovos-agent-plugin
pip install -e .[test]
```

## Running tests

```bash
pytest tests/ -v
```

The test suite uses `FakeBus` from `ovos-utils` and `MagicMock` for client connections, so it does not need a running OVOS instance or hivemind-core node.

### Coverage

```bash
pytest tests/ --cov=hivemind_ovos_agent_plugin --cov-report=term-missing
```

## Branching and releases

- `dev` is the active development branch. All PRs target `dev`.
- `master` carries tagged releases. Direct pushes to `master` are reserved for the publish workflow.
- Alpha builds publish to PyPI automatically when a PR merges to `dev` (see `.github/workflows/release_workflow.yml`).
- Stable releases publish when changes land on `master` (see `.github/workflows/publish_stable.yml`).

Versions live in `hivemind_ovos_agent_plugin/version.py`. The publish workflows bump them automatically.

## Adding tests

Tests live under `tests/`. Follow these conventions:

- Unit tests use `FakeBus` and never start a real websocket server.
- Mock `client.send` to capture outbound HiveMessages instead of running a real network protocol.
- Add a test for each behavior in `docs/message_flow.md`. If you add a new code path, add a test under the matching filename.

## Release checklist

1. Open PRs against `dev`.
2. Merge to `dev`. Alpha publishes automatically.
3. When ready for stable, the "Release Alpha and Propose Stable" workflow opens a PR from `dev` to `master`.
4. Merge that PR. Stable publishes automatically and the version bumps.

---
[← Message flow](message_flow.md) · [Home](README.md) · [Policy →](policy.md)
