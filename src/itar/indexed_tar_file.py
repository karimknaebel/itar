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

IndexedTarIndex = dict[str, (int, MemberRecord)]  # fname -> (shard_idx, MemberRecord)
Shard = str | os.PathLike | IO[bytes]


def _normalize_shards(
    shards: list[Shard] | Shard,
) -> tuple[bool, list[Shard], list[bool]]:
    is_sharded = isinstance(shards, list)
    normalized = list(shards) if is_sharded else [shards]
    needs_open = [isinstance(s, (str, os.PathLike)) for s in normalized]
    return is_sharded, normalized, needs_open


class IndexedTarFile(Mapping):
    def __init__(
        self,
        shards: list[Shard] | Shard,
        index: IndexedTarIndex,
        open_fn: Callable[[str | os.PathLike], IO[bytes]] = None,
        buffered_file_reader: bool = True,
    ):
        if index is None:
            raise ValueError("index must be provided")

        _, shards, self._needs_open = _normalize_shards(shards)
        self._file_reader = BufferedReader if buffered_file_reader else lambda x: x
        open_fn = (
            open_fn or ThreadSafeFileIO
        )  # In our benchmarks, `ThreadSafeFileIO` is even faster than `partial(open, mode="rb", buffering=0)`. Likely due to `pread` being fewer syscalls than `seek` + `read`.
        self._shard_file_objs: list[IO[bytes]] = [
            open_fn(tar) if needs_open else tar
            for tar, needs_open in zip(shards, self._needs_open, strict=True)
        ]
        self._index = index

    def file(self, name: str) -> IO[bytes]:
        i, member = self._index[name]
        _, offset_data, size = member
        if isinstance(size, str):
            return self.file(size)  # symlink or hard link
        return self._file_reader(
            TarFileSectionIO(self._shard_file_objs[i], offset_data, size)
        )

    def info(self, name: str) -> TarInfo:
        i, member = self._index[name]
        offset, _, _ = member
        return tar_file_info(offset, self._shard_file_objs[i])

    def check_tar_index(self, names: list[str] | None = None):
        for name in names if names is not None else self:
            i, member = self._index[name]
            check_tar_index(name, member, self._shard_file_objs[i])

    def close(self):
        for needed_open, file_obj in zip(
            self._needs_open, self._shard_file_objs, strict=True
        ):
            if needed_open:
                # only close what we opened
                file_obj.close()

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
