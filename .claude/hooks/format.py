"""PostToolUse hook: format the file Claude just edited.

Python files: ruff format + ruff check --fix (via uv, so the project venv is used).
C/C++ files: clang-format -i, only if clang-format is on PATH.
Never fails the tool call: formatting problems are reported, not blocking.
"""

import json
import shutil
import subprocess
import sys
from pathlib import Path

PY = {".py", ".pyi"}
CPP = {".cpp", ".cc", ".cxx", ".hpp", ".hh", ".h", ".ipp"}


def main() -> int:
    try:
        payload = json.load(sys.stdin)
    except json.JSONDecodeError:
        return 0
    file_path = payload.get("tool_input", {}).get("file_path")
    if not file_path:
        return 0
    path = Path(file_path)
    if not path.exists():
        return 0

    if path.suffix in PY and shutil.which("uv"):
        subprocess.run(["uv", "run", "ruff", "format", str(path)], check=False)
        subprocess.run(["uv", "run", "ruff", "check", "--fix", str(path)], check=False)
    elif path.suffix in CPP and shutil.which("clang-format"):
        subprocess.run(["clang-format", "-i", str(path)], check=False)
    return 0


if __name__ == "__main__":
    sys.exit(main())
