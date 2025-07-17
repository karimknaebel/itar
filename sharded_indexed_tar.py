import os
from collections.abc import Collection, Mapping
from contextlib import contextmanager
from tarfile import TarInfo
from typing import IO

from .indexed_tar import IndexedTar, TarIndex


class ShardedIndexedTar(Mapping):
    def __init__(
        self,
        shards: list[str | os.PathLike | IO[bytes]],
        indices: list[TarIndex | None] | None = None,
    ):
        if indices is None:
            indices = [None] * len(shards)
        self._shards = [
            IndexedTar(tar, index) for tar, index in zip(shards, indices, strict=True)
        ]
        self._shard_index = {
            name: idx for idx, shard in enumerate(self._shards) for name in shard
        }

    def shard(self, name: str) -> IndexedTar:
        return self._shards[self._shard_index[name]]

    def file(self, name: str) -> IO[bytes]:
        """
        Note: all files in the same shard share the same underlying file object,
        so it is not safe to read from multiple files in the same shard concurrently.
        """
        return self.shard(name).file(name)

    def info(self, name: str) -> TarInfo:
        return self.shard(name).info(name)

    def verify_index(self, name: str):
        return self.shard(name).verify_index(name)

    @property
    def indices(self) -> list[TarIndex]:
        return [shard.index for shard in self._shards]

    def close(self):
        for itar in self._shards:
            itar.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def __getitem__(self, name: str):
        return self.file(name)

    def __contains__(self, name: str) -> bool:
        return name in self._shard_index

    def __iter__(self):
        return iter(self._shard_index)

    def __len__(self):
        return len(self._shard_index)

    def keys(self):
        return self._shard_index.keys()

    def values(self):
        for name in self._shard_index:
            yield self[name]

    def items(self):
        for name in self._shard_index:
            yield (name, self[name])


class SafeShardedIndexedTar(Collection):
    """
    This is a thread-safe version of ShardedIndexedTar. Every operation uses a new file descriptor.
    """

    def __init__(
        self,
        shards: list[str | os.PathLike | IO[bytes]],
        indices: list[TarIndex | None] | None = None,
    ):
        if indices is None:
            indices = [None] * len(shards)
        self._shards = shards
        self._indices = [
            IndexedTar(tar, index).index
            for tar, index in zip(shards, indices, strict=True)
        ]
        self._shard_index = {
            name: idx for idx, index in enumerate(indices) for name in index
        }

    def shard(self, name: str) -> IndexedTar:
        return IndexedTar(
            tar=self._shards[self._shard_index[name]],
            index=self._indices[self._shard_index[name]],
        )

    @contextmanager
    def open(self, name: str) -> IO[bytes]:
        with (
            self.shard(name) as shard,
            shard.file(name) as f,  # this is not really necessary
        ):
            yield f

    def info(self, name: str) -> TarInfo:
        with self.shard(name) as shard:
            return shard.info(name)

    def verify_index(self, name: str):
        with self.shard(name) as shard:
            return shard.verify_index(name)

    @property
    def indices(self) -> list[TarIndex]:
        return self._indices

    def __contains__(self, name: str) -> bool:
        return name in self._shard_index

    def __iter__(self):
        return iter(self._shard_index)

    def __len__(self):
        return len(self._shard_index)
