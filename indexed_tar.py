import os
import tarfile
from collections.abc import Mapping
from tarfile import ExFileObject, TarFile, TarInfo
from types import SimpleNamespace
from typing import IO

TarIndex = dict[
    str, tuple[int, int, int, bool]
]  # name -> (offset, offset_data, size, sparse)


class IndexedTar(Mapping):
    def __init__(
        self, tar: str | os.PathLike | IO[bytes], index: TarIndex | None = None
    ):
        self._needed_open = isinstance(tar, str | os.PathLike)
        self._tar_file_obj = open(tar, "rb") if self._needed_open else tar
        self._index = index if index is not None else self._build_index()

    def file(self, name: str) -> IO[bytes]:
        _, offset_data, size, sparse = self._index[name]
        return ExFileObject(
            SimpleNamespace(fileobj=self._tar_file_obj),
            SimpleNamespace(
                offset_data=offset_data, size=size, name=name, sparse=sparse
            ),
        )

    @classmethod
    def _resolve(
        cls, member: TarInfo, index: dict[str, TarInfo], offset: int | None = None
    ) -> TarInfo:
        if member.issym():
            return cls._resolve(
                index[
                    "/".join(
                        filter(None, (os.path.dirname(member.name), member.linkname))
                    )
                ],
                index,
                offset=member.offset,
            )
        elif member.islnk():
            return cls._resolve(index[member.linkname], index, offset=member.offset)
        return (
            offset if offset is not None else member.offset,
            member.offset_data,
            member.size,
            member.sparse,
        )

    def info(self, name: str) -> TarInfo:
        offset, _, _, _ = self._index[name]
        self._tar_file_obj.seek(offset)
        return TarInfo.fromtarfile(
            # want to avoid creating a new TarFile instance (potentially slow)
            SimpleNamespace(
                fileobj=self._tar_file_obj,
                # would be the defaults after TarFile.__init__
                encoding=TarFile.encoding,
                errors="surrogateescape",
                pax_headers={},
            )
        )

    def verify_index(self, name: str):
        """
        Check if the index matches the tar file for a given name.
        Raises ValueError if there is a mismatch.
        """
        offset, offset_data, size, sparse = self._index[name]
        info = self.info(name)
        if info.offset != offset or (
            not (info.islnk() or info.issym())
            and (
                info.name != name
                or info.offset_data != offset_data
                or info.size != size
                or info.sparse != sparse
            )
        ):
            raise ValueError(
                f"Index mismatch: "
                f"expected ({name}, {offset}, {offset_data}, {size}, {sparse}), "
                f"got ({info.name}, {info.offset}, {info.offset_data}, {info.size}, {info.sparse})"
            )

    @property
    def index(self) -> TarIndex:
        return self._index

    def _build_index(self) -> TarIndex:
        self._tar_file_obj.seek(0)
        members = {
            member.name: member
            for member in tarfile.open(
                fileobj=self._tar_file_obj, mode="r:"
            ).getmembers()
        }

        return {
            member.name: self._resolve(member, members) for member in members.values()
        }

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
