import builtins
import os
import re
from pathlib import Path
from typing import IO, Callable

from .indexed_tar_file import IndexedTarFile, IndexedTarIndex, Shard, ShardResolver
from .utils import build_tar_index


class DuplicateShardError(ValueError):
    pass


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

    def discover_shards(self) -> dict[int, Path]:
        pattern = re.compile(rf"{re.escape(self.stem)}-([0-9]+)\.tar")
        shards = {}
        for path in self._index_path.parent.iterdir():
            match = pattern.fullmatch(path.name)
            if match is None or not path.is_file():
                continue
            idx = int(match[1])
            if idx in shards:
                raise DuplicateShardError(
                    f"Duplicate shard ID {idx}: {shards[idx]} and {path}"
                )
            shards[idx] = path
        return dict(sorted(shards.items()))


class DefaultResolver:
    """Resolve shard indices to paths next to an existing index file."""

    def __init__(self, layout: IndexLayout):
        self.layout = layout
        self._shards: dict[int, Path] | None = None

    def __call__(self, shard_idx: int | None) -> Path:
        if shard_idx is None:
            return self.layout.single_tar()

        if self._shards is None:
            self._shards = self.layout.discover_shards()

        try:
            return self._shards[shard_idx]
        except KeyError:
            raise FileNotFoundError(
                f"Shard {shard_idx} not found for {self.layout.index_path}"
            ) from None


def build(
    shards: dict[int, Shard] | list[Shard] | Shard,
    *,
    progress_bar: bool = False,
) -> IndexedTarIndex:
    """Build an index using dictionary IDs, list positions, or a single TAR."""
    if isinstance(shards, dict):
        sources = shards
    elif isinstance(shards, list):
        sources = dict(enumerate(shards))
    else:
        sources = {None: shards}
    iterator = sources.items()
    if progress_bar:
        from tqdm import tqdm

        iterator = tqdm(iterator, desc="Building index", unit="shard")

    return {
        name: (shard_id, member)
        for shard_id, source in iterator
        for name, member in build_tar_index(source).items()
    }


def load(path: str | os.PathLike) -> IndexedTarIndex:
    """Load an index dictionary from a saved ``.itar`` index file."""

    import msgspec

    path = Path(path)
    with builtins.open(path, "rb") as f:
        return msgspec.msgpack.decode(f.read(), type=IndexedTarIndex)


def dump(
    index: IndexedTarIndex,
    path: str | os.PathLike,
) -> None:
    """Persist ``index`` to disk in MessagePack format."""

    import msgspec

    path = Path(path)
    with builtins.open(path, "wb") as f:
        f.write(msgspec.msgpack.encode(index))


def open(
    path: str | os.PathLike,
    shards: dict[int, Shard] | list[Shard] | Shard | None = None,
    open_fn: Callable[[str | os.PathLike], IO[bytes]] | None = None,
    buffered_file_reader: bool = True,
) -> IndexedTarFile:
    """Open an ``IndexedTarFile`` using an on-disk index file."""

    path = Path(path)
    index = load(path)
    layout = IndexLayout(path)
    resolved_shards: dict[int, Shard] | list[Shard] | Shard | ShardResolver
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
    shards: dict[int, Shard] | list[Shard] | Shard,
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
