import os
from collections.abc import Mapping
from io import BufferedReader
from pathlib import Path
from tarfile import TarInfo
from typing import IO, Callable

from .utils import (
    TarFileSectionIO,
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
        buffered_file_reader: bool = True,
        progress_bar: bool = False,
    ):
        """
        For best performance, we recommend using unbuffered shard IO in combination with buffered individual file section readers.
        In our (limited) benchmarks, this was the fastest configuration for reading many small files.
        It it possbile that in other scenarios, buffered shard IO and unbuffered file section readers are faster.
        """
        self._needs_open = [isinstance(s, str | os.PathLike) for s in shards]
        self._file_reader = BufferedReader if buffered_file_reader else lambda x: x
        open_fn = (
            open_fn or ThreadSafeFileIO
        )  # In our benchmarks, `ThreadSafeFileIO` is even faster than `partial(open, mode="rb", buffering=0)`. Likely due to `pread` being fewer syscalls than `seek` + `read`.
        self._shard_file_objs: IO[bytes] = [
            open_fn(tar) if needs_open else tar
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
        open_fn: Callable[[str | os.PathLike], IO[bytes]] = None,
        buffered_file_reader: bool = True,
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
            open_fn=open_fn,
            buffered_file_reader=buffered_file_reader,
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

    @property
    def index(self) -> ShardedTarIndex:
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

    parser = argparse.ArgumentParser(description="Create or check an itar index.")
    subparsers = parser.add_subparsers(dest="command")

    create_parser = subparsers.add_parser("create", help="Create an itar index")
    create_parser.add_argument(
        "itar", type=Path, help="Path to the new itar index that will be created"
    )

    check_parser = subparsers.add_parser("check", help="Check an existing itar index")
    check_parser.add_argument("itar", type=Path, help="Path to the itar index")

    ls_parser = subparsers.add_parser("ls", help="List files in an itar index")
    ls_parser.add_argument("itar", type=Path, help="Path to the itar index")
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
    import re

    itar_path = Path(args.itar)
    stem = itar_path.stem
    pattern = re.compile(rf"^{re.escape(stem)}-\d+\.tar$")
    shards = sorted(
        [
            s
            for s in itar_path.parent.glob(f"{itar_path.stem}-*.tar")
            if s.is_file() and pattern.match(s.name)
        ]
    )
    num_shards = len(shards)
    if num_shards < 1:
        print(
            f"No shards found for {itar_path}.\n"
            f"Please create shard files first. Expected pattern: "
            f"'{itar_path.stem}-NN.tar' (where NN is the zero-padded shard index, starting from 0)."
        )
        exit(1)

    # ensure shards are named correctly
    expected = [
        ShardedIndexedTar.shard_path(itar_path, num_shards, i)
        for i in range(num_shards)
    ]
    assert shards == expected, (
        f"Shards do not match expected names: {shards} != {expected}"
    )

    with ShardedIndexedTar(shards, progress_bar=True) as itar:
        itar.save(itar_path)
    print(f"Created itar index at {itar_path} with {num_shards} shards.")


def _check(args):
    from tqdm import tqdm

    did_error = False

    with ShardedIndexedTar.open(args.itar) as itar:
        for member in tqdm(itar, desc="Checking files", unit="file"):
            try:
                itar.check_tar_index([member])
            except TarIndexError as e:
                print(e)
                did_error = True

    if did_error:
        exit(1)


def _ls(args):
    with ShardedIndexedTar.open(args.itar) as itar:
        index = itar.index
    if args.long:
        for member, (shard_idx, (offset, offset_data, size)) in index.items():
            if args.human_readable:
                from humanize import naturalsize

                size = naturalsize(size, gnu=True)
            print(
                f"{member:<40} {shard_idx:>5} {offset:>12} {offset_data:>12} {size:>10}"
            )
        print(f"{'NAME':<40} {'SHARD':>5} {'OFFSET':>12} {'OFF_DATA':>12} {'SIZE':>10}")
    else:
        for member in index:
            print(member)
