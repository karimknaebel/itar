import io
import os
import tarfile
import threading
from contextlib import nullcontext
from tarfile import TarFile, TarInfo
from types import SimpleNamespace
from typing import IO, BinaryIO

TarMember = tuple[int, int, int | str]  # (offset, offset_data, size | linkname)


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
        size = os.path.normpath(
            "/".join(filter(None, (os.path.dirname(tarinfo.name), tarinfo.linkname)))
        )
    elif tarinfo.islnk():
        size = os.path.normpath(tarinfo.linkname)
    else:
        size = tarinfo.size

    if tarinfo.sparse is not None:
        raise NotImplementedError("Sparse files are not supported")

    return (tarinfo.offset, tarinfo.offset_data, size)


def build_tar_index(
    tar: str | os.PathLike | IO[bytes] | TarFile,
) -> dict[str, TarMember]:
    if isinstance(tar, str | os.PathLike):
        tar = tarfile.open(tar, "r:")
    elif isinstance(tar, TarFile):
        tar = nullcontext(tar)
    else:
        tar.seek(0)
        tar = tarfile.open(fileobj=tar, mode="r:")

    with tar as f:
        members = {member.name: member for member in f.getmembers()}
        return {
            member.name: tarinfo2member(member)
            for member in members.values()
            if (
                # index only includes files and links. no directories, devices, etc.
                member.isreg()
                or (
                    member.type not in tarfile.SUPPORTED_TYPES
                )  # Members with unknown types are treated as regular files.
                or member.issym()
                or member.islnk()
            )
        }


class TarIndexError(Exception):
    pass


def check_tar_index(
    name: str,
    tar_offset: TarMember,
    file_obj: IO[bytes],
):
    offset, offset_data, size = tar_offset
    info = tar_file_info(offset, file_obj)
    if (
        info.offset != offset
        or info.name != name
        or info.offset_data != offset_data
        or (info.size != size and not info.islnk() and not info.issym())
    ):
        # TODO: if link, check that linkname is correct
        raise TarIndexError(
            f"Index mismatch: "
            f"expected ({name}, {offset}, {offset_data}, {size}), "
            f"got ({info.name}, {info.offset}, {info.offset_data}, {info.size})"
        )


class ThreadSafeFileIO(io.RawIOBase):
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


class TarFileSectionIO(io.RawIOBase):
    def __init__(self, fileobj: BinaryIO, offset: int, size: int):
        self._fileobj = fileobj
        self._start = offset
        self._end = offset + size
        self._pos = 0  # relative position within the slice

    def read(self, size: int = -1) -> bytes:
        if self._pos >= self._end - self._start:
            return b""

        abs_pos = self._start + self._pos
        max_len = self._end - (self._start + self._pos)

        if size < 0 or size > max_len:
            size = max_len

        self._fileobj.seek(abs_pos)
        data = self._fileobj.read(size)
        self._pos += len(data)
        return data

    def readinto(self, b: bytearray) -> int:
        data = self.read(len(b))
        n = len(data)
        b[:n] = data
        return n

    def readall(self) -> bytes:
        return self.read(-1)

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        if whence == io.SEEK_SET:
            new_pos = offset
        elif whence == io.SEEK_CUR:
            new_pos = self._pos + offset
        elif whence == io.SEEK_END:
            new_pos = (self._end - self._start) + offset
        else:
            raise ValueError(f"Invalid whence: {whence}")

        if new_pos < 0:
            raise ValueError("Negative seek position")
        self._pos = min(new_pos, self._end - self._start)
        return self._pos

    def tell(self) -> int:
        return self._pos

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return True

    def writable(self) -> bool:
        return False

    def close(self) -> None:
        pass  # Don't close the underlying file

    def __len__(self) -> int:
        return self._end - self._start
