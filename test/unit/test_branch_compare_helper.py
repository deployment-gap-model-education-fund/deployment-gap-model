"""Unit tests for the branch_compare_helper script."""

from pathlib import Path
from unittest import mock

import pytest

from scripts import branch_compare_helper as bch


def test_resolve_command_success():
    """Test that resolve_command returns a valid path for a known command."""
    assert Path(bch.resolve_command("ls")).exists()


def test_resolve_command_failure():
    """Test that resolve_command raises an error for an unknown command."""
    with pytest.raises(RuntimeError, match="Command not found"):
        bch.resolve_command("nonexistent_command_xyz")


@mock.patch("scripts.branch_compare_helper.resolve_command", return_value="/bin/echo")
@mock.patch("scripts.branch_compare_helper.subprocess.run")
def test_run_command(mock_run, mock_resolve):
    """Test that run_command correctly resolves and executes a shell command."""
    bch.run_command("echo hello")
    mock_run.assert_called_once()
    assert mock_resolve.called


@mock.patch(
    "scripts.branch_compare_helper.resolve_command", return_value="/usr/bin/git"
)
@mock.patch("scripts.branch_compare_helper.subprocess.check_output")
def test_get_current_branch(mock_check_output, mock_resolve):
    """Test that get_current_branch returns the correct branch name."""
    mock_check_output.return_value = b"main\n"
    assert bch.get_current_branch() == "main"


def test_get_modified_files(tmp_path):
    """Test that get_modified_files returns recently modified files."""
    f1 = tmp_path / "data/output/data_mart/file1.txt"
    f1.parent.mkdir(parents=True)
    f1.write_text("test")

    modified_files = bch.get_modified_files(tmp_path, since_time=0)
    assert f1 in modified_files


def test_copy_files(tmp_path):
    """Test that copy_files copies files while preserving relative paths."""
    source = tmp_path / "source_dir/a/b/file.txt"
    source.parent.mkdir(parents=True)
    source.write_text("hello")

    dest_dir = tmp_path / "dest_dir"
    bch.copy_files([source], dest_dir)

    copied = dest_dir / "b/file.txt"
    assert copied.exists()
    assert copied.read_text() == "hello"


@mock.patch("scripts.branch_compare_helper.run_command")
@mock.patch("scripts.branch_compare_helper.get_modified_files", return_value=[])
def test_collect_artifacts(mock_get_files, mock_run, tmp_path):
    """Test that collect_artifacts runs build steps and copies output files."""
    branch = "feature-branch"
    bch.collect_artifacts(branch, output_dir=str(tmp_path), tmp_root=str(tmp_path))

    assert mock_run.call_count == 2  # checkout and make all
    assert mock_get_files.called


@mock.patch("scripts.branch_compare_helper.get_current_branch", return_value="main")
@mock.patch("scripts.branch_compare_helper.collect_artifacts")
@mock.patch("scripts.branch_compare_helper.run_command")
def test_main(mock_run, mock_collect, mock_get_branch):
    """Test that main switches branches, collects artifacts, and returns to the original."""
    bch.main("feature-xyz", "dev")

    mock_collect.assert_any_call("feature-xyz")
    mock_collect.assert_any_call("dev")
    mock_run.assert_called_with("git checkout main")
