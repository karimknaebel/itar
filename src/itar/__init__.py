from . import index
from .indexed_tar_file import IndexedTarFile


open = index.open


__all__ = [
    "IndexedTarFile",
    "index",
    "open",
]
