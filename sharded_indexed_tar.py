import io
import os
from collections.abc import Mapping
from pathlib import Path
from tarfile import TarInfo
from typing import IO, Callable

from .utils import (
    TarFileSectionIO,
    TarIndex,
    TarIndexError,
    TarMember,
    ThreadSafeFileIO,
    build_tar_index,
    check_tar_index,
    tar_file_info,
)

ShardedTarIndex = dict[str, (int, TarMember)]  # fname -> (shard_idx, TarMember)


class ShardedIndexedTar(Mapping):
    def __init__(
        self,
        shards: list[str | os.PathLike | IO[bytes]],
        index: ShardedTarIndex | None = None,
        open_fn: Callable[[str | os.PathLike], IO[bytes]] = None,
        progress_bar: bool = False,
    ):
        self._needs_open = [isinstance(s, str | os.PathLike) for s in shards]
        # NOTE: We buffer individual file sections, not whole shards.
        # Initially benchmarks showed that the default `open` buffer size of 8192 noticably slows down reading of many small files.
        self._shard_file_objs: IO[bytes] = [
            (open_fn(tar) if open_fn else open(tar, "rb", buffering=0))
            if needs_open
            else tar
            for tar, needs_open in zip(shards, self._needs_open, strict=True)
        ]
        if progress_bar:
            from tqdm import tqdm
        else:
            tqdm = lambda x, **kwargs: x  # noqa: E731
        self._index = (
            index
            if index is not None
            else {
                name: (i, member)
                for i, file_obj in enumerate(
                    tqdm(self._shard_file_objs, desc="Building index", unit="shard")
                )
                for name, member in build_tar_index(file_obj).items()
            }
        )

    @classmethod
    def open(
        cls,
        path: str | os.PathLike,
        shards: list[str | os.PathLike] | None = None,
        thread_safe: bool = False,
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
            open_fn=ThreadSafeFileIO if thread_safe else None,
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
        i, member = self._index[name]
        _, offset_data, size = member
        if isinstance(size, str):
            return self.file(size)  # symlink or hard link
        tar_file_section = TarFileSectionIO(self._shard_file_objs[i], offset_data, size)
        return (
            io.BufferedReader(tar_file_section)  # our file objects are unbuffered
            if self._needs_open[i]
            else tar_file_section  # we make no assumptions about buffering of external file objects
        )

    def info(self, name: str) -> TarInfo:
        i, member = self._index[name]
        offset, _, _ = member
        return tar_file_info(offset, self._shard_file_objs[i])

    def check_tar_index(self, names: list[str] | None = None):
        for name in names if names is not None else self:
            i, member = self._index[name]
            check_tar_index(name, member, self._shard_file_objs[i])

    @property
    def index(self) -> list[TarIndex]:
        return self._index

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


def cli():
    import argparse

    parser = argparse.ArgumentParser(description="Create or check a sitar index.")
    subparsers = parser.add_subparsers(dest="command")

    create_parser = subparsers.add_parser("create", help="Create a sitar index")
    create_parser.add_argument(
        "shards", nargs="+", type=Path, help="Paths to the tar shards"
    )

    check_parser = subparsers.add_parser("check", help="Check an existing sitar index")
    check_parser.add_argument("sitar", type=Path, help="Path to the sitar index file")

    ls_parser = subparsers.add_parser("ls", help="List files in a sitar index")
    ls_parser.add_argument("sitar", type=Path, help="Path to the sitar index file")
    ls_parser.add_argument(
        "-l", "--long", action="store_true", help="Show long listing format"
    )
    ls_parser.add_argument(
        "-H", "--human-readable", action="store_true", help="Use human-readable sizes"
    )

    args = parser.parse_args()

    if args.command == "create":
        _create(args)
    elif args.command == "check":
        _check(args)
    elif args.command == "ls":
        _ls(args)


def _create(args):
    num_shards = len(args.shards)
    assert num_shards > 0

    path = (
        args.shards[0].parent / args.shards[0].stem[: -len(str(num_shards - 1)) - 1]
    ).with_suffix(".sitar")

    # ensure shards are named correctly
    for i, shard in enumerate(args.shards):
        assert shard == ShardedIndexedTar.shard_path(path, num_shards, i)

    with ShardedIndexedTar(args.shards, progress_bar=True) as sitar:
        sitar.save(path)


def _check(args):
    from tqdm import tqdm

    did_error = False

    with ShardedIndexedTar.open(args.sitar) as sitar:
        for member in tqdm(sitar, desc="Checking files", unit="file"):
            try:
                sitar.check_tar_index([member])
            except TarIndexError as e:
                print(e)
                did_error = True

    if did_error:
        exit(1)


def _ls(args):
    with ShardedIndexedTar.open(args.sitar) as sitar:
        if args.long:
            for member in sitar:
                shard_idx, (offset, offset_data, size) = sitar.index[member]
                if args.human_readable:
                    from humanize import naturalsize

                    size = naturalsize(size, gnu=True)
                print(
                    f"{member:<40} {shard_idx:>5} {offset:>12} {offset_data:>12} {size:>10}"
                )
            print(
                f"{'NAME':<40} {'SHARD':>5} {'OFFSET':>12} {'OFF_DATA':>12} {'SIZE':>10}"
            )
        else:
            for member in sitar:
                print(member)
