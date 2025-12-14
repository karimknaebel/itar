import builtins
import os
import re
from collections.abc import Iterable
from pathlib import Path
from typing import IO, Callable

from .indexed_tar_file import IndexedTarFile, IndexedTarIndex, Shard, ShardResolver
from .utils import build_tar_index


class IndexLayout:
    """Naming helpers for the TAR shards backing an index file."""

    def __init__(self, index_path: str | os.PathLike):
        self._index_path = Path(index_path)

    @property
    def index_path(self) -> Path:
        return self._index_path

    @property
    def stem(self) -> str:
        return self._index_path.stem

    def single_tar(self) -> Path:
        return self._index_path.with_suffix(".tar")

    def shard(self, shard_idx: int, total_shards: int) -> Path:
        if total_shards <= 0:
            raise ValueError("total_shards must be positive")
        width = max(1, len(str(total_shards - 1)))
        return self._index_path.parent / f"{self.stem}-{shard_idx:0{width}d}.tar"

    def shards(self, total_shards: int) -> list[Path]:
        return [self.shard(i, total_shards) for i in range(total_shards)]

    def discover_shards(self) -> list[Path]:
        pattern = re.compile(rf"^{re.escape(self.stem)}-\d+\.tar$")
        candidates = [
            path
            for path in self._index_path.parent.glob(f"{self.stem}-*.tar")
            if path.is_file() and pattern.match(path.name)
        ]
        candidates.sort()
        return candidates


class DefaultResolver:
    """Resolve shard indices to paths next to an existing index file."""

    def __init__(self, layout: IndexLayout):
        self.layout = layout

    def __call__(self, shard_idx: int | None) -> Path:
        if shard_idx is None:
            return self.layout.single_tar()

        shard_paths = self.layout.discover_shards()
        return shard_paths[shard_idx]


def _build_index_from_fileobjs(
    file_objs: Iterable[IO[bytes]],
    *,
    progress_bar: bool,
    use_shard_indices: bool,
) -> IndexedTarIndex:
    iterator = file_objs
    if progress_bar:
        from tqdm import tqdm

        iterator = tqdm(file_objs, desc="Building index", unit="shard")

    return {
        name: ((i if use_shard_indices else None), member)
        for i, file_obj in enumerate(iterator)
        for name, member in build_tar_index(file_obj).items()
    }


def build(
    shards: list[Shard] | Shard,
    *,
    progress_bar: bool = False,
) -> IndexedTarIndex:
    """Build an index mapping without instantiating ``IndexedTarFile``."""
    is_sharded = isinstance(shards, list)
    if not is_sharded:
        shards = [shards]
    needs_open = [isinstance(s, (str, os.PathLike)) for s in shards]

    file_objs: list[IO[bytes]] = [
        builtins.open(tar, "rb") if needs else tar
        for tar, needs in zip(shards, needs_open, strict=True)
    ]
    try:
        return _build_index_from_fileobjs(
            file_objs,
            progress_bar=progress_bar,
            use_shard_indices=is_sharded,
        )
    finally:
        for needs, file_obj in zip(needs_open, file_objs, strict=True):
            if needs:
                file_obj.close()


def load(path: str | os.PathLike) -> IndexedTarIndex:
    """Load an index dictionary from a saved ``.itar`` index file."""

    import msgpack

    path = Path(path)
    with builtins.open(path, "rb") as f:
        return msgpack.load(f)


def dump(
    index: IndexedTarIndex,
    path: str | os.PathLike,
) -> None:
    """Persist ``index`` to disk in msgpack format."""

    import msgpack

    path = Path(path)
    with builtins.open(path, "wb") as f:
        msgpack.dump(index, f)


def open(
    path: str | os.PathLike,
    shards: list[Shard] | Shard | None = None,
    open_fn: Callable[[str | os.PathLike], IO[bytes]] | None = None,
    buffered_file_reader: bool = True,
) -> IndexedTarFile:
    """Open an ``IndexedTarFile`` using an on-disk index file."""

    path = Path(path)
    index = load(path)
    layout = IndexLayout(path)
    resolved_shards: list[Shard] | Shard | ShardResolver
    if shards is not None:
        resolved_shards = shards
    else:
        resolved_shards = DefaultResolver(layout)
    return IndexedTarFile(
        resolved_shards,
        index=index,
        open_fn=open_fn,
        buffered_file_reader=buffered_file_reader,
    )


def create(
    path: str | os.PathLike,
    shards: list[Shard] | Shard,
    *,
    progress_bar: bool = True,
) -> IndexedTarIndex:
    """Build an index for ``shards`` and save it to ``path``."""

    index = build(shards, progress_bar=progress_bar)
    dump(index, path)
    return index


__all__ = [
    "create",
    "open",
    "build",
    "dump",
    "load",
    "IndexLayout",
]
