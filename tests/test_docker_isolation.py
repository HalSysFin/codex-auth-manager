from __future__ import annotations

import unittest
from pathlib import Path


class DockerIsolationTests(unittest.TestCase):
    def test_compose_uses_named_volumes_for_codex_state(self) -> None:
        compose = Path("docker-compose.yml").read_text()
        self.assertIn("- codex_auth:/root/.codex", compose)
        self.assertIn("- codex_switch:/root/.codex-switch", compose)
        self.assertNotIn("- ~/.codex:/root/.codex", compose)
        self.assertNotIn("- ~/.codex-switch", compose)


if __name__ == "__main__":
    unittest.main()
