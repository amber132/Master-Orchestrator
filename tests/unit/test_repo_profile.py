from pathlib import Path

from claude_orchestrator.repo_profile import RepoProfiler


def test_repo_profiler_detects_mixed_stack_and_doc_drift(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "pom.xml").write_text("<project><artifactId>x</artifactId><groupId>a</groupId></project>", encoding="utf-8")
    (repo / "mvnw").write_text("", encoding="utf-8")
    frontend = repo / "frontend"
    frontend.mkdir()
    (frontend / "package.json").write_text(
        '{"dependencies":{"react":"19.0.0"},"devDependencies":{"@vitejs/plugin-react":"4.0.0"},"scripts":{"build":"vite build","lint":"eslint ."}}',
        encoding="utf-8",
    )
    (frontend / "vite.config.ts").write_text("import react from '@vitejs/plugin-react'", encoding="utf-8")
    (repo / "CLAUDE.md").write_text("- **前端**：Vue 3 + Vite\n", encoding="utf-8")

    profile = RepoProfiler().profile(repo)

    assert profile.has_backend is True
    assert profile.has_frontend is True
    assert "spring-boot" in profile.detected_frameworks
    assert "react" in profile.detected_frameworks
    assert profile.documentation_drift is True


def test_repo_profiler_extracts_verification_commands(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "package.json").write_text(
        '{"scripts":{"build":"vite build","lint":"eslint .","test":"vitest"}}',
        encoding="utf-8",
    )

    profile = RepoProfiler().profile(repo)
    assert "npm run build" in profile.verification_commands
    assert "npm run lint" in profile.verification_commands



def test_repo_profiler_detects_backup_hints_from_sample_app_style_repo(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "docker-compose.yml").write_text(
        """version: '3.8'
services:
  backend:
    volumes:
      - ./logs:/app/logs
      - ./uploads:/app/uploads
""",
        encoding="utf-8",
    )
    resources = repo / "src" / "main" / "resources"
    resources.mkdir(parents=True)
    (resources / "application-dev.yml").write_text(
        """spring:
  datasource:
    url: jdbc:postgresql://localhost:5432/sample_app?stringtype=unspecified
    username: root
    password: ignored
""",
        encoding="utf-8",
    )

    profile = RepoProfiler().profile(repo)

    assert "uploads" in profile.file_backup_paths
    assert any("pg_dump" in command for command in profile.database_backup_commands)
    assert any("sample_app" in command for command in profile.database_backup_commands)
