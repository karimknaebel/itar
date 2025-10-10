import builtins
import os
import re
from collections.abc import Iterable
from pathlib import Path
from typing import IO, Callable

from .indexed_tar_file import (
    IndexedTarFile,
    IndexedTarIndex,
    Shard,
    _normalize_shards,
)
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


def _build_index_from_fileobjs(
    file_objs: Iterable[IO[bytes]],
    *,
    progress_bar: bool,
) -> IndexedTarIndex:
    iterator = file_objs
    if progress_bar:
        from tqdm import tqdm

        iterator = tqdm(file_objs, desc="Building index", unit="shard")

    return {
        name: (i, member)
        for i, file_obj in enumerate(iterator)
        for name, member in build_tar_index(file_obj).items()
    }


def build(
    shards: list[Shard] | Shard,
    *,
    progress_bar: bool = False,
) -> IndexedTarIndex:
    """Build an index mapping without instantiating ``IndexedTarFile``."""

    _, shard_list, needs_open = _normalize_shards(shards)
    file_objs: list[IO[bytes]] = [
        builtins.open(tar, "rb") if needs else tar
        for tar, needs in zip(shard_list, needs_open, strict=True)
    ]
    try:
        return _build_index_from_fileobjs(file_objs, progress_bar=progress_bar)
    finally:
        for needs, file_obj in zip(needs_open, file_objs, strict=True):
            if needs:
                file_obj.close()


def load(path: str | os.PathLike) -> tuple[int | None, IndexedTarIndex]:
    """Load an index dictionary and shard count from a saved ``.itar`` index file."""

    import msgpack

    path = Path(path)
    with builtins.open(path, "rb") as f:
        num_shards, index = msgpack.load(f)
    return num_shards, index


def save(
    path: str | os.PathLike,
    num_shards: int | None,
    index: IndexedTarIndex,
) -> None:
    """Persist ``(num_shards, index)`` to disk in msgpack format."""

    import msgpack

    path = Path(path)
    with builtins.open(path, "wb") as f:
        msgpack.dump((num_shards, index), f)


def _infer_default_shards(path: Path, num_shards: int | None) -> list[Shard] | Shard:
    layout = IndexLayout(path)
    if num_shards is None:
        return layout.single_tar()
    return layout.shards(num_shards)


def open(
    path: str | os.PathLike,
    shards: list[Shard] | Shard | None = None,
    open_fn: Callable[[str | os.PathLike], IO[bytes]] | None = None,
    buffered_file_reader: bool = True,
) -> IndexedTarFile:
    """Open an :class:`IndexedTarFile` using an on-disk index file."""

    path = Path(path)
    num_shards, index = load(path)
    resolved_shards = (
        shards if shards is not None else _infer_default_shards(path, num_shards)
    )
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
    is_sharded, normalized, _ = _normalize_shards(shards)
    num_shards = len(normalized) if is_sharded else None
    save(path, num_shards, index)
    return index


__all__ = [
    "IndexLayout",
    "build",
    "create",
    "load",
    "open",
    "save",
]
