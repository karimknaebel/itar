import os
from collections.abc import Mapping
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
