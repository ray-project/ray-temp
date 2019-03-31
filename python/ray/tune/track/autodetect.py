import os
import subprocess
import sys
import shlex


def git_repo():
    """Returns Git repository root if the cwd is in a repo else None."""
    try:
        with open(os.devnull, 'wb') as quiet:
            reldir = subprocess.check_output(
                ["git", "rev-parse", "--git-dir"],
                stdout=quiet)
        reldir = reldir.decode("utf-8")
        return os.path.basename(os.path.dirname(os.path.abspath(reldir)))
    except subprocess.CalledProcessError:
        return None


def git_hash():
    """Returns the current git hash or None if not in git repo"""
    if git_repo() is None:
        return None
    git_hash = subprocess.check_output(
        ["git", "rev-parse", "HEAD"])
    # git_hash is a byte string; we want a string.
    git_hash = git_hash.decode("utf-8")
    # git_hash also comes with an extra \n at the end, which we remove.
    git_hash = git_hash.strip()
    return git_hash


def git_pretty():
    """Returns a pretty summary of the commit or None if not in git repo"""
    if git_repo() is None:
        return None
    pretty = subprocess.check_output(
        ["git", "log", "--pretty=format:%h %s", "-n", "1"])
    pretty = pretty.decode("utf-8")
    pretty = pretty.strip()
    return pretty


def invocation():
    """Returns the invocation for this python program."""
    cmdargs = [sys.executable] + sys.argv[:]
    invocation = " ".join(shlex.quote(s) for s in cmdargs)
    return invocation
