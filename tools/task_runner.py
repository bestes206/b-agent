"""Task automation tool — runs shell commands and manages simple tasks."""

import subprocess


def run_command(command: str, timeout: int = 30) -> str:
    """Run a shell command and return its output.

    Args:
        command: The shell command to execute.
        timeout: Maximum seconds to wait (default 30).

    Returns:
        The command's stdout/stderr output, or an error message.
    """
    try:
        result = subprocess.run(
            command,
            shell=True,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        output = result.stdout
        if result.stderr:
            output += f"\n[stderr]: {result.stderr}"
        if result.returncode != 0:
            output += f"\n[exit code]: {result.returncode}"
        return output.strip() or "(no output)"
    except subprocess.TimeoutExpired:
        return f"Command timed out after {timeout}s"
    except Exception as e:
        return f"Command failed: {e}"


def run_python(code: str) -> str:
    """Execute a Python code snippet and return its output.

    Args:
        code: Python code to execute.

    Returns:
        The printed output from the code, or an error message.
    """
    return run_command(f'python3 -c {subprocess.list2cmdline([code])}', timeout=30)
