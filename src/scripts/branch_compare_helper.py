"""Script to help with handling data outputs to compare branches."""

import shutil
import subprocess  # nosec
import time
from pathlib import Path


def resolve_command(cmd):
    """Resolve the command to prevent user insertion of malicious commands."""
    full_path = shutil.which(cmd)
    if not full_path:
        raise RuntimeError(f"Command not found: {cmd}")
    return full_path


def run_command(cmd, cwd=None):
    """Run shell command passed as a list."""
    cmd_list = cmd if isinstance(cmd, list) else cmd.split(" ")
    cmd_list[0] = resolve_command(cmd_list[0])
    subprocess.run(cmd_list, check=True, cwd=cwd)  # nosec


def get_current_branch():
    """Return currently checked out git branch as a string."""
    git_path = resolve_command("git")

    return (
        (
            subprocess.check_output(  # nosec
                [git_path, "rev-parse", "--abbrev-ref", "HEAD"]
            )
        )
        .decode()
        .strip()
    )


def get_modified_files(directory, since_time):
    """List all files modified during current run."""
    return [
        f
        for f in Path(directory).rglob("*")
        if f.is_file() and f.stat().st_mtime >= since_time
    ]


def copy_files(files, dest_dir):
    """Copy files from output to temporary directory to compare."""
    dest_dir = Path(dest_dir)
    dest_dir.mkdir(parents=True, exist_ok=True)
    for f in files:
        relative = f.relative_to(f.parents[1])  # Customize based on directory depth
        dest_path = dest_dir / relative
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(f, dest_path)


def collect_artifacts(branch, output_dir="data/output/data_mart", tmp_root="data/tmp"):
    """Collect all artifacts to compare target branch to base branch output."""
    print(f"🔀 Checking out {branch}")
    run_command(f"git checkout {branch}")

    print("🛠️  Running `make all`...")
    start_time = time.time()
    run_command("make all")

    print(f"📦 Collecting files from {output_dir}")
    files = get_modified_files(output_dir, since_time=start_time)

    tmp_path = Path(tmp_root) / branch
    print(f"📁 Copying to {tmp_path}")
    copy_files(files, tmp_path)


def main(target_branch, base_branch="dev"):
    """Script to compare target branch to base branch."""
    original_branch = get_current_branch()

    try:
        collect_artifacts(target_branch)
        collect_artifacts(base_branch)
    finally:
        print(f"🔁 Returning to original branch: {original_branch}")
        run_command(f"git checkout {original_branch}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--target_branch", help="Branch to compare (default: current branch)"
    )
    parser.add_argument(
        "--base_branch",
        default="dev",
        help="Base branch to compare against (default: dev)",
    )
    args = parser.parse_args()

    target = args.target_branch or get_current_branch()
    main(target_branch=target, base_branch=args.base_branch)
