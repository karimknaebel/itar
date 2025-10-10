# itar

[![image](https://img.shields.io/pypi/v/itar.svg)](https://pypi.python.org/pypi/itar)

`itar` builds constant‑time indexes over one or more tar file shards, enabling direct, random access to members without extracting the archives. It ships a lightweight CLI (`itar`) and a Python API.

Designed for large datasets and deep‑learning pipelines, it supports single or sharded tar archives with thread‑safe access for concurrent reads.

## Quickstart (single tarball)

```bash
echo "Hello world!" > hello.txt
tar cf hello.tar hello.txt       # regular tarball
itar index create hello.itar     # indexes hello.tar
itar index list hello.itar       # list indexed members
```

```python
import itar

with itar.open("hello.itar") as archive:
    print(archive["hello.txt"].read())

# If you just need the index dictionary without opening handles:
index = itar.index.build("hello.tar")
# Write the index file to disk:
itar.index.save("hello.itar", num_shards=None, index=index)
```

## Quickstart (sharded tarballs)

Give each shard a zero-padded suffix before building the index:

```bash
tar cf photos-0.tar wedding/   # shard 0
tar cf photos-1.tar vacation/  # shard 1
itar index create photos.itar  # discovers photos-0.tar, photos-1.tar, ...
itar index list -l photos.itar # shard index, offsets, byte sizes
```

```python
import itar

with itar.open("photos.itar") as photos:
    assert "wedding/cake.jpg" in photos
    img_bytes = photos["vacation/sunrise.jpg"].read()

index = itar.index.build(["photos-0.tar", "photos-1.tar"])
itar.index.create("photos.itar", ["photos-0.tar", "photos-1.tar"])

num_shards, stored_index = itar.index.load("photos.itar")
```

## CLI reference

| Command | Purpose |
| --- | --- |
| `itar index create <archive>.itar [--single TAR \| --shards shard0.tar shard1.tar ...]` | Indexes a single archive or an explicit set of shards. With no flags, shards are auto-discovered next to `<archive>.itar`. |
| `itar index list <archive>.itar` | Lists members. Use `-l` for shard/offset info and `-H` for human-readable sizes. |
| `itar index check <archive>.itar` | Validates recorded entries; add `--member NAME` to focus on specific files. |
| `itar cat <archive>.itar <member>` | Streams a member’s bytes to stdout. |

## Python helpers

- `itar.index.build(shards, progress_bar=False) -> dict`: construct an index mapping for paths, file objects, or buffers.
- `itar.index.create("archive.itar", shards)`: convenience wrapper that builds + saves an index file.
- `itar.index.save(path, num_shards, index)`: serialize an index you built elsewhere.
- `itar.index.load(path) -> (num_shards, index)`: load the msgpack index without opening shards.
- `itar.open(path, *, shards=None, open_fn=None) -> IndexedTarFile`: attach shard handles using an existing index file.
