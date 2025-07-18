import os
from collections.abc import Mapping
from pathlib import Path
from tarfile import TarInfo
from typing import IO

from .indexed_tar import TarIndex
from .utils import (
    TarIndexError,
    TarMember,
    build_tar_index,
    check_tar_index,
    tar_file_info,
    tar_file_reader,
)

ShardedTarIndex = dict[str, (int, TarMember)]  # fname -> (shard_idx, TarMember)


class ShardedIndexedTar(Mapping):
    def __init__(
        self,
        shards: list[str | os.PathLike | IO[bytes]],
        index: ShardedTarIndex | None = None,
    ):
        self._needed_open = [isinstance(s, str | os.PathLike) for s in shards]
        self._shard_file_objs: IO[bytes] = [
            open(tar, "rb") if needs_open else tar
            for tar, needs_open in zip(shards, self._needed_open, strict=True)
        ]
        self._index = (
            index
            if index is not None
            else {
                name: (i, member)
                for i, file_obj in enumerate(self._shard_file_objs)
                for name, member in build_tar_index(file_obj).items()
            }
        )

    @classmethod
    def open(
        cls, path: str | os.PathLike, shards: list[str | os.PathLike] | None = None
    ):
        import msgpack

        path = Path(path)
        with open(path, "rb") as f:
            num_shards, index = msgpack.load(f)
        return cls(
            shards
            if shards is not None
            else [cls.shard_path(path, num_shards, i) for i in range(num_shards)],
            index,
        )

    def save(self, path: str | os.PathLike):
        import msgpack

        path = Path(path)
        with open(path, "wb") as f:
            msgpack.dump((len(self._shard_file_objs), self.index), f)

    @staticmethod
    def shard_path(path: str | os.PathLike, num_shards: int, shard_idx: int) -> Path:
        path = Path(path)
        return path.parent / f"{path.stem}-{shard_idx:0{len(str(num_shards - 1))}d}.tar"

    def _shard(self, name: str) -> tuple[IO[bytes], TarMember]:
        i, member = self._index[name]
        return self._shard_file_objs[i], member

    def file(self, name: str) -> IO[bytes]:
        file_obj, member = self._shard(name)
        _, offset_data, size, sparse = member
        return tar_file_reader(name, offset_data, size, sparse, file_obj)

    def info(self, name: str) -> TarInfo:
        file_obj, member = self._shard(name)
        offset, _, _, _ = member
        return tar_file_info(offset, file_obj)

    def check_tar_index(self, names: list[str] | None = None):
        for name in names if names is not None else self:
            file_obj, member = self._shard(name)
            check_tar_index(name, member, file_obj)

    @property
    def index(self) -> list[TarIndex]:
        return self._index

    def close(self):
        for needed_open, file_obj in zip(
            self._needed_open, self._shard_file_objs, strict=True
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


def cli_create():
    import argparse

    parser = argparse.ArgumentParser(description="Create a sitar index.")
    parser.add_argument("shards", nargs="+", type=Path, help="Paths to the tar shards")
    args = parser.parse_args()

    num_shards = len(args.shards)
    assert num_shards > 0

    path = (
        args.shards[0].parent / args.shards[0].stem[: -len(str(num_shards - 1)) - 1]
    ).with_suffix(".sitar")

    # ensure shards are named correctly
    for i, shard in enumerate(args.shards):
        assert shard == ShardedIndexedTar.shard_path(path, num_shards, i)

    with ShardedIndexedTar(args.shards) as sitar:
        sitar.save(path)


def cli_check():
    import argparse

    from tqdm import tqdm

    parser = argparse.ArgumentParser(description="Check an existing sitar index.")
    parser.add_argument("sitar", type=Path, help="Paths to the sitar index file")
    args = parser.parse_args()

    return_code = 0

    with ShardedIndexedTar.open(args.sitar) as sitar:
        for member in tqdm(sitar, desc="Checking files"):
            try:
                sitar.check_tar_index([member])
            except TarIndexError as e:
                print(e)
                return_code = 1

    return return_code
