import re

import pygit2


POINTER_PATTERN = re.compile(rb"^oid sha256:([0-9a-fA-F]{64})$", re.MULTILINE)


def get_hash_from_pointer_file(pointer_file_bytes):
    """Given a pointer-file Blob or bytes object, extracts the sha256 hash from it."""
    if isinstance(pointer_file_bytes, pygit2.Blob):
        pointer_file_bytes = memoryview(pointer_file_bytes)
    match = POINTER_PATTERN.search(pointer_file_bytes)
    if match:
        return str(match.group(1), encoding="utf8")
    return None


def get_local_path_from_lfs_hash(repo, lfs_hash):
    """Given a sha256 LFS hash, finds where the object would be stored in the local LFS cache."""
    return (
        repo.gitdir_path / "lfs" / "objects" / lfs_hash[0:2] / lfs_hash[2:4] / lfs_hash
    )