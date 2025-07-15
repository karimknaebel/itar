import os
from contextlib import nullcontext
from pathlib import Path
from typing import IO

import msgpack
from mini.itar.indexed_tar import IndexedTar, TarIndex
from mini.itar.sharded_indexed_tar import ShardedIndexedTar


class SITar(ShardedIndexedTar):
    def __init__(self, path: str | os.PathLike, fileobj: IO[bytes] | None = None):
        path = Path(path)
        with open(path, "rb") if fileobj is None else nullcontext(fileobj) as f:
            shards, indices = msgpack.load(f)
        super().__init__(
            shards=[path.parent / shard for shard in shards],
            indices=indices,
        )

    @staticmethod
    def save(
        path: str | os.PathLike,
        shards: list[str | os.PathLike],
        indices: list[TarIndex | None] | None = None,
        fileobj: IO[bytes] | None = None,
    ):
        path = Path(path)
        if indices is None:
            indices = [None] * len(shards)
        indices = [
            IndexedTar(tar, None).index if index is None else index
            for tar, index in zip(shards, indices, strict=True)
        ]
        with open(path, "wb") if fileobj is None else nullcontext(fileobj) as f:
            msgpack.dump(
                ([str(Path(s).relative_to(path.parent)) for s in shards], indices),
                f,
            )


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Create a SITar archive.")
    parser.add_argument("path", type=Path, help="Path to save the SITar archive")
    parser.add_argument("shards", nargs="+", type=Path, help="Paths to the tar shards")
    args = parser.parse_args()

    SITar.save(args.path, args.shards)
