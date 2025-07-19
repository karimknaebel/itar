import io
import os
import tarfile
import threading
from contextlib import nullcontext
from tarfile import ExFileObject, TarFile, TarInfo
from types import SimpleNamespace
from typing import IO

TarMember = tuple[int, int, int | str, bool]  # (offset, offset_data, size, sparse)
TarIndex = dict[str, TarMember]  # fname -> TarOffset


def tar_file_reader(
    name: str,
    offset_data: int,
    size: int,
    sparse: list[tuple[int, int]] | None,
    file_obj: IO[bytes],
) -> IO[bytes]:
    return ExFileObject(
        SimpleNamespace(fileobj=file_obj),
        SimpleNamespace(offset_data=offset_data, size=size, name=name, sparse=sparse),
    )


def tar_file_info(offset: int, file_obj: IO[bytes]) -> TarInfo:
    file_obj.seek(offset)
    return TarInfo.fromtarfile(
        # want to avoid creating a new TarFile instance (potentially slow)
        SimpleNamespace(
            fileobj=file_obj,
            # would be the defaults after TarFile.__init__
            encoding=TarFile.encoding,
            errors="surrogateescape",
            pax_headers={},
        )
    )


def tarinfo2member(tarinfo: TarInfo) -> TarMember:
    if tarinfo.issym():
        size = "/".join(filter(None, (os.path.dirname(tarinfo.name), tarinfo.linkname)))
    elif tarinfo.islnk():
        size = tarinfo.linkname
    else:
        size = tarinfo.size

    return (
        tarinfo.offset,
        tarinfo.offset_data,
        size,
        tarinfo.sparse,
    )


def build_tar_index(tar: str | os.PathLike | IO[bytes] | TarFile) -> TarIndex:
    if isinstance(tar, str | os.PathLike):
        tar = tarfile.open(tar, "r:")
    elif isinstance(tar, TarFile):
        tar = nullcontext(tar)
    else:
        tar.seek(0)
        tar = tarfile.open(fileobj=tar, mode="r:")

    with tar as f:
        members = {member.name: member for member in f.getmembers()}
        return {member.name: tarinfo2member(member) for member in members.values()}


class TarIndexError(Exception):
    pass


def check_tar_index(
    name: str,
    tar_offset: TarMember,
    file_obj: IO[bytes],
):
    offset, offset_data, size, sparse = tar_offset
    info = tar_file_info(offset, file_obj)
    if (
        info.offset != offset
        or info.name != name
        or info.offset_data != offset_data
        or (info.size != size and not info.islnk() and not info.issym())
        or info.sparse != sparse
    ):
        raise TarIndexError(
            f"Index mismatch: "
            f"expected ({name}, {offset}, {offset_data}, {size}, {sparse}), "
            f"got ({info.name}, {info.offset}, {info.offset_data}, {info.size}, {info.sparse})"
        )


class ThreadLocalPreadIO(io.RawIOBase):
    """
    A thread-safe, file-like object that wraps a file descriptor
    and uses os.pread() for concurrent reads.

    Each thread has its own seek position.
    """

    def __init__(self, path: str | os.PathLike):
        self._path = str(path)
        self._fd = os.open(path, os.O_RDONLY)
        self._local = threading.local()
        self._closed = False

    def _get_pos(self) -> int:
        return getattr(self._local, "pos", 0)

    def _set_pos(self, val: int) -> None:
        self._local.pos = val

    def seek(self, offset: int, whence: int = os.SEEK_SET) -> int:
        pos = self._get_pos()
        if whence == os.SEEK_SET:
            self._set_pos(offset)
        elif whence == os.SEEK_CUR:
            self._set_pos(pos + offset)
        elif whence == os.SEEK_END:
            end = os.lseek(self._fd, 0, os.SEEK_END)
            self._set_pos(end + offset)
        else:
            raise ValueError(f"Invalid whence: {whence}")
        return self._get_pos()

    def tell(self) -> int:
        return self._get_pos()

    def read(self, size: int = -1) -> bytes:
        if size < 0:
            raise NotImplementedError("read(size=-1) not supported")
        pos = self._get_pos()
        data = os.pread(self._fd, size, pos)
        self._set_pos(pos + len(data))
        return data

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return True

    def writable(self) -> bool:
        return False

    def close(self) -> None:
        if not self._closed:
            os.close(self._fd)
            self._closed = True

    def fileno(self) -> int:
        return self._fd

    @property
    def name(self) -> str:  # optional
        return self._path

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, tb):
        self.close()
