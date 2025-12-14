import os
from collections.abc import Mapping
from io import BufferedReader
from tarfile import TarInfo
from typing import IO, Callable

from .utils import (
    MemberRecord,
    TarFileSectionIO,
    ThreadSafeFileIO,
    check_tar_index,
    tar_file_info,
)

IndexedTarIndex = dict[
    str, (int | None, MemberRecord)
]  # fname -> (shard_idx, MemberRecord)
Shard = str | os.PathLike | IO[bytes]
ShardResolver = Callable[[int | None], Shard]


class IndexedTarFile(Mapping):
    """
    Read-only mapping that serves members out of indexed tar shards.

    Args:
        shards: Shard sources (single tar, list of tars, or resolver callable).
        index: Precomputed index mapping member names to offsets.
        open_fn: Optional callable to open paths; defaults to a thread-safe file reader.
        buffered_file_reader: Wrap member streams in a buffered reader when True.

    Use ``itar.open`` to construct this class. It supports the mapping protocol
    for read access (``archive["path/to/file"]``) and should be used as a
    context manager to close any open file handles.

    Example:
        ```python
        import itar

        with itar.open("photos.itar") as archive:
            jpg = archive["vacation/sunrise.jpg"].read()
            assert "wedding/cake.jpg" in archive
        ```
    """

    def __init__(
        self,
        shards: list[Shard] | Shard | ShardResolver,
        index: IndexedTarIndex,
        open_fn: Callable[[str | os.PathLike], IO[bytes]] = None,
        buffered_file_reader: bool = True,
    ):
        if index is None:
            raise ValueError("index must be provided")

        self._file_reader = BufferedReader if buffered_file_reader else lambda x: x
        self._open_fn = (
            open_fn or ThreadSafeFileIO
        )  # In our benchmarks, `ThreadSafeFileIO` is even faster than `partial(open, mode="rb", buffering=0)`. Likely due to `pread` being fewer syscalls than `seek` + `read`.
        self._index = index

        self._resolver: ShardResolver
        self._handles: dict[int | None, IO[bytes]] = {}
        self._closable: set[int | None] = set()

        if callable(shards):
            self._resolver = shards  # type: ignore[assignment]
        else:
            sources = (
                {idx: shard for idx, shard in enumerate(shards)}
                if isinstance(shards, list)
                else {None: shards}
            )

            def resolver(idx: int | None) -> Shard:
                return sources[idx]

            self._resolver = resolver

    def _ensure_shard(self, shard_idx: int | None) -> IO[bytes]:
        if shard_idx in self._handles:
            return self._handles[shard_idx]

        source = self._resolver(shard_idx)
        if isinstance(source, (str, os.PathLike)):
            handle = self._open_fn(source)
            self._handles[shard_idx] = handle
            self._closable.add(shard_idx)
            return handle

        self._handles[shard_idx] = source
        return source

    def file(self, name: str) -> IO[bytes]:
        """Return a readable file-like object for an indexed member."""
        shard_idx, member = self._index[name]
        _, offset_data, size = member
        if isinstance(size, str):
            return self.file(size)  # symlink or hard link
        return self._file_reader(
            TarFileSectionIO(self._ensure_shard(shard_idx), offset_data, size)
        )

    def info(self, name: str) -> TarInfo:
        """Return the ``TarInfo`` for an indexed member without reading data."""
        shard_idx, member = self._index[name]
        offset, _, _ = member
        return tar_file_info(offset, self._ensure_shard(shard_idx))

    def check_tar_index(self, names: list[str] | None = None):
        """Validate stored offsets for the given names (or all members)."""
        for name in names if names is not None else self:
            shard_idx, member = self._index[name]
            check_tar_index(name, member, self._ensure_shard(shard_idx))

    def close(self):
        for key in self._closable:
            self._handles[key].close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def __getitem__(self, name: str):
        return self.file(name)

    def __contains__(self, name: str) -> bool:
        return name in self._index

    def __iter__(self):
        return iter(self._index)

    def __len__(self):
        return len(self._index)

    def keys(self):
        return self._index.keys()

    def values(self):
        for name in self._index:
            yield self[name]

    def items(self):
        for name in self._index:
            yield (name, self[name])
