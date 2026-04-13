"""Repository profiling for automatic task planning."""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class RepoProfile:
    root: Path
    has_backend: bool = False
    has_frontend: bool = False
    backend_dir: Path | None = None
    frontend_dir: Path | None = None
    detected_frameworks: list[str] = field(default_factory=list)
    package_managers: list[str] = field(default_factory=list)
    verification_commands: list[str] = field(default_factory=list)
    backend_commands: list[str] = field(default_factory=list)
    frontend_commands: list[str] = field(default_factory=list)
    documentation_drift: bool = False
    warnings: list[str] = field(default_factory=list)
    docker_compose_file: str = ""
    file_backup_paths: list[str] = field(default_factory=list)
    metadata_backup_paths: list[str] = field(default_factory=list)
    database_backup_commands: list[str] = field(default_factory=list)


class RepoProfiler:
    def profile(self, root: str | Path) -> RepoProfile:
        repo_root = Path(root).resolve()
        profile = RepoProfile(root=repo_root)

        self._detect_backend(profile)

        frontend_dir = repo_root / "frontend"
        package_root = frontend_dir if (frontend_dir / "package.json").exists() else repo_root if (repo_root / "package.json").exists() else None
        package_data: dict = {}
        if package_root:
            try:
                package_data = json.loads((package_root / "package.json").read_text(encoding="utf-8"))
            except (json.JSONDecodeError, OSError):
                package_data = {}

            deps = {
                **package_data.get("dependencies", {}),
                **package_data.get("devDependencies", {}),
            }
            scripts = package_data.get("scripts", {})
            has_frontend_package = bool(scripts) or package_root.name == "frontend"
            vite_config = package_root / "vite.config.ts"
            backend_framework = self._detect_node_backend_framework(deps, package_root)
            is_explicit_frontend = (
                "react" in deps
                or "vue" in deps
                or (vite_config.exists() and "plugin-react" in vite_config.read_text(encoding="utf-8", errors="ignore"))
            )
            package_manager = self._node_package_manager(package_root, package_data)

            if backend_framework and package_root.name != "frontend" and not is_explicit_frontend:
                profile.detected_frameworks.append(backend_framework)
                profile.has_backend = True
                profile.backend_dir = package_root
                profile.package_managers.append(package_manager)
                if "test" in scripts:
                    profile.backend_commands.append(self._package_script_command(package_manager, "test"))
                if "build" in scripts:
                    profile.backend_commands.append(self._package_script_command(package_manager, "build"))
                if "lint" in scripts:
                    profile.backend_commands.append(self._package_script_command(package_manager, "lint"))
            elif "react" in deps or (vite_config.exists() and "plugin-react" in vite_config.read_text(encoding="utf-8", errors="ignore")):
                profile.detected_frameworks.append("react")
                profile.has_frontend = True
            elif "vue" in deps:
                profile.detected_frameworks.append("vue")
                profile.has_frontend = True
            elif has_frontend_package:
                profile.detected_frameworks.append("frontend-package")
                profile.has_frontend = True

            if profile.has_frontend:
                profile.frontend_dir = package_root
                profile.package_managers.append(package_manager)
                if "build" in scripts:
                    profile.frontend_commands.append(self._package_script_command(package_manager, "build"))
                if "lint" in scripts:
                    profile.frontend_commands.append(self._package_script_command(package_manager, "lint"))
                if "test" in scripts:
                    profile.frontend_commands.append(self._package_script_command(package_manager, "test"))

        compose_file = repo_root / "docker-compose.yml"
        if compose_file.exists():
            profile.docker_compose_file = str(compose_file)

        self._detect_documentation_drift(profile)
        self._detect_backup_hints(profile)
        generic_commands = self._extract_generic_commands(repo_root)

        profile.detected_frameworks = sorted(dict.fromkeys(profile.detected_frameworks))
        profile.package_managers = sorted(dict.fromkeys(profile.package_managers))
        profile.backend_commands = list(dict.fromkeys(profile.backend_commands))
        profile.frontend_commands = list(dict.fromkeys(profile.frontend_commands))
        profile.file_backup_paths = list(dict.fromkeys(profile.file_backup_paths))
        profile.metadata_backup_paths = list(dict.fromkeys(profile.metadata_backup_paths))
        profile.database_backup_commands = list(dict.fromkeys(profile.database_backup_commands))
        profile.verification_commands = (
            profile.backend_commands
            + [cmd for cmd in profile.frontend_commands if cmd not in profile.backend_commands]
            + [cmd for cmd in generic_commands if cmd not in profile.backend_commands and cmd not in profile.frontend_commands]
        )
        return profile

    def _detect_backend(self, profile: RepoProfile) -> None:
        repo_root = profile.root
        if (repo_root / "pom.xml").exists() or (repo_root / "build.gradle").exists():
            profile.has_backend = True
            profile.backend_dir = repo_root
            profile.package_managers.append("maven" if (repo_root / "pom.xml").exists() else "gradle")
            profile.detected_frameworks.append("spring-boot")
            mvn_cmd = self._mvn_command(repo_root)
            profile.backend_commands.extend([
                f"{mvn_cmd} -q -DskipTests compile",
                f"{mvn_cmd} test",
            ])

        pyproject = repo_root / "pyproject.toml"
        if pyproject.exists() or (repo_root / "requirements.txt").exists() or (repo_root / "setup.py").exists():
            profile.has_backend = True
            profile.backend_dir = repo_root
            profile.package_managers.append("python")
            profile.detected_frameworks.append("python")
            has_python_tooling = False
            pyproject_text = ""
            if pyproject.exists():
                try:
                    pyproject_text = pyproject.read_text(encoding="utf-8", errors="ignore").lower()
                except OSError:
                    pyproject_text = ""
            if "pytest" in pyproject_text or (repo_root / "tests").exists():
                profile.backend_commands.append("python -m pytest")
                has_python_tooling = True
            if "ruff" in pyproject_text:
                profile.backend_commands.append("ruff check .")
                has_python_tooling = True
            if "mypy" in pyproject_text:
                profile.backend_commands.append("mypy .")
                has_python_tooling = True
            if not has_python_tooling:
                profile.backend_commands.append("python -m compileall .")

        if (repo_root / "go.mod").exists():
            profile.has_backend = True
            profile.backend_dir = repo_root
            profile.package_managers.append("go")
            profile.detected_frameworks.append("go")
            profile.backend_commands.extend([
                "go test ./...",
                "go build ./...",
            ])

        if (repo_root / "Cargo.toml").exists():
            profile.has_backend = True
            profile.backend_dir = repo_root
            profile.package_managers.append("cargo")
            profile.detected_frameworks.append("rust")
            profile.backend_commands.extend([
                "cargo test",
                "cargo check",
            ])

    def _extract_generic_commands(self, repo_root: Path) -> list[str]:
        for filename, prefix in (("Makefile", "make"), ("justfile", "just"), ("Justfile", "just")):
            commands = self._extract_target_commands(repo_root / filename, prefix)
            if commands:
                return commands
        return []

    def _extract_target_commands(self, path: Path, prefix: str) -> list[str]:
        if not path.exists():
            return []
        try:
            text = path.read_text(encoding="utf-8", errors="ignore")
        except OSError:
            return []
        supported_targets = {"test", "build", "lint", "check", "verify"}
        commands: list[str] = []
        for match in re.finditer(r"^([A-Za-z0-9_.-]+)\s*:(?![=])", text, flags=re.MULTILINE):
            target = match.group(1)
            if target in supported_targets:
                commands.append(f"{prefix} {target}")
        return commands

    def _detect_node_backend_framework(self, deps: dict[str, object], package_root: Path) -> str:
        backend_markers = (
            ("@nestjs/core", "nestjs"),
            ("@nestjs/common", "nestjs"),
            ("express", "express"),
            ("fastify", "fastify"),
            ("koa", "koa"),
            ("@hapi/hapi", "hapi"),
            ("hono", "hono"),
        )
        dep_names = {str(name).lower() for name in deps}
        for marker, framework in backend_markers:
            if marker in dep_names:
                return framework

        server_hints = (
            package_root / "server.js",
            package_root / "server.ts",
            package_root / "src" / "server.ts",
            package_root / "src" / "server.js",
        )
        if any(path.exists() for path in server_hints):
            return "node-backend"
        return ""

    def _node_package_manager(self, package_root: Path, package_data: dict) -> str:
        package_manager = str(package_data.get("packageManager", "")).strip().lower()
        if package_manager.startswith("pnpm@"):
            return "pnpm"
        if package_manager.startswith("yarn@"):
            return "yarn"
        if package_manager.startswith("npm@"):
            return "npm"
        if (package_root / "pnpm-lock.yaml").exists():
            return "pnpm"
        if (package_root / "yarn.lock").exists():
            return "yarn"
        return "npm"

    def _package_script_command(self, package_manager: str, script: str) -> str:
        if package_manager == "yarn":
            return f"yarn {script}"
        return f"{package_manager} run {script}"

    def _detect_documentation_drift(self, profile: RepoProfile) -> None:
        claude_file = profile.root / "CLAUDE.md"
        if not claude_file.exists():
            return
        try:
            text = claude_file.read_text(encoding="utf-8", errors="ignore").lower()
        except OSError:
            return
        if "vue" in text and "react" in profile.detected_frameworks:
            profile.documentation_drift = True
            profile.warnings.append("文档仍声明 Vue，但实际检测到 React 前端")
        if "react" in text and "vue" in profile.detected_frameworks:
            profile.documentation_drift = True
            profile.warnings.append("文档仍声明 React，但实际检测到 Vue 前端")

    def _detect_backup_hints(self, profile: RepoProfile) -> None:
        self._collect_compose_backup_paths(profile)
        self._collect_common_backup_paths(profile)
        self._collect_database_backup_commands(profile)

    def _collect_compose_backup_paths(self, profile: RepoProfile) -> None:
        if not profile.docker_compose_file:
            return
        try:
            text = Path(profile.docker_compose_file).read_text(encoding="utf-8", errors="ignore")
        except OSError:
            return

        file_tokens = ("upload", "storage", "data", "asset", "media", "file")
        metadata_tokens = ("meta", "metadata", "manifest")
        ignore_tokens = ("log", "node_modules", "target", "build", "dist", "frontend", "src")

        for raw_path in re.findall(r"^\s*-\s+\./([^:\n]+):", text, flags=re.MULTILINE):
            normalized = raw_path.strip().replace("\\", "/").strip("/")
            if not normalized:
                continue
            base_name = Path(normalized).name.lower()
            if any(token in base_name for token in ignore_tokens):
                continue
            if any(token in base_name for token in metadata_tokens):
                profile.metadata_backup_paths.append(normalized)
                continue
            if any(token in base_name for token in file_tokens):
                profile.file_backup_paths.append(normalized)

    def _collect_common_backup_paths(self, profile: RepoProfile) -> None:
        common_file_paths = ("uploads", "upload", "storage", "data", "assets", "media")
        common_metadata_paths = ("metadata", "manifests")

        for name in common_file_paths:
            candidate = profile.root / name
            if candidate.exists() or name in profile.file_backup_paths:
                profile.file_backup_paths.append(name)

        for name in common_metadata_paths:
            candidate = profile.root / name
            if candidate.exists() or name in profile.metadata_backup_paths:
                profile.metadata_backup_paths.append(name)

    def _collect_database_backup_commands(self, profile: RepoProfile) -> None:
        resources_dir = profile.root / "src" / "main" / "resources"
        if not resources_dir.exists():
            return

        app_files = [
            resources_dir / "application-dev.yml",
            resources_dir / "application-test.yml",
            resources_dir / "application.yml",
            resources_dir / "application-prod.yml",
        ]
        for app_file in app_files:
            if not app_file.exists():
                continue
            try:
                text = app_file.read_text(encoding="utf-8", errors="ignore")
            except OSError:
                continue

            match = re.search(r"jdbc:postgresql://(?P<host>[^:/?\s]+)(?::(?P<port>\d+))?/(?P<db>[^?\s]+)", text)
            if not match:
                continue

            user_match = re.search(r"^\s*username:\s*([^\s#]+)", text, flags=re.MULTILINE)
            host = match.group("host")
            port = match.group("port") or "5432"
            database = match.group("db")
            username = user_match.group(1) if user_match else "postgres"
            profile.database_backup_commands.append(
                self._build_postgres_backup_command(host=host, port=port, database=database, username=username)
            )
            return

    def _build_postgres_backup_command(self, host: str, port: str, database: str, username: str) -> str:
        if os.name == "nt":
            return (
                f'set "PGHOST={host}" && '
                f'set "PGPORT={port}" && '
                f'set "PGUSER={username}" && '
                f'set "PGDATABASE={database}" && '
                'pg_dump --no-password --file "{output}"'
            )
        return (
            f"PGHOST='{host}' PGPORT='{port}' PGUSER='{username}' PGDATABASE='{database}' "
            "pg_dump --no-password --file \"{output}\""
        )

    def _mvn_command(self, root: Path) -> str:
        if os.name == "nt":
            if (root / "mvnw.cmd").exists():
                return "mvnw.cmd"
            return "mvn"
        if (root / "mvnw").exists():
            return "./mvnw"
        return "mvn"