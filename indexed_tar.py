import os
from collections.abc import Mapping
from pathlib import Path
from tarfile import TarInfo
from typing import IO

from .utils import (
    TarIndex,
    build_tar_index,
    check_tar_index,
    tar_file_info,
    tar_file_reader,
)


class IndexedTar(Mapping):
    def __init__(
        self,
        tar: str | os.PathLike | IO[bytes],
        index: TarIndex | None = None,
    ):
        self._needed_open = isinstance(tar, str | os.PathLike)
        self._tar_file_obj: IO[bytes] = open(tar, "rb") if self._needed_open else tar
        self._index = (
            index if index is not None else build_tar_index(self._tar_file_obj)
        )

    @classmethod
    def open(cls, path: str | os.PathLike, tar: str | os.PathLike | None = None):
        import msgpack

        path = Path(path)
        with open(path, "rb") as f:
            index = msgpack.load(f)
        return cls(tar if tar is not None else path.with_suffix(".tar"), index)

    def save(self, path: str | os.PathLike):
        import msgpack

        path = Path(path)
        with open(path, "wb") as f:
            msgpack.dump(self.index, f)

    def file(self, name: str) -> IO[bytes]:
        _, offset_data, size, sparse = self._index[name]
        return tar_file_reader(name, offset_data, size, sparse, self._tar_file_obj)

    def info(self, name: str) -> TarInfo:
        offset, _, _, _ = self._index[name]
        return tar_file_info(offset, self._tar_file_obj)

    def check_tar_index(self):
        for name in self._index:
            check_tar_index(name, self._index[name], self._tar_file_obj)

    @property
    def index(self) -> TarIndex:
        return self._index

    def close(self):
        if self._needed_open:
            # only close what we opened
            self._tar_file_obj.close()

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
