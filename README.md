# itar

`itar` builds constant-time indexes for one or more TAR shards so you can seek directly to a member without extracting the archive. The project ships a small CLI (`itar`) and a Python helper (`ShardedIndexedTar`) that hands out file-like objects for each indexed member.

## Quickstart (single tarball)

```bash
echo "Hello world!" > hello.txt
tar cf hello.tar hello.txt       # regular tarball
itar create hello.itar           # indexes hello.tar
itar ls hello.itar               # list indexed members
```

```python
from itar import ShardedIndexedTar

with ShardedIndexedTar.open("hello.itar") as itar:
    assert itar["hello.txt"].read() == b"Hello world!\n"
```

## Quickstart (sharded tarballs)

Give each shard a zero-padded suffix before building the index:

```bash
tar cf photos-0.tar wedding/   # shard 0
tar cf photos-1.tar vacation/  # shard 1
itar create photos.itar        # discovers photos-0.tar, photos-1.tar, ...
itar ls -l photos.itar         # shard index, offsets, byte sizes
```

```python
from itar import ShardedIndexedTar

with ShardedIndexedTar.open("photos.itar") as photos:
    assert "wedding/cake.jpg" in photos
    img_bytes = photos["vacation/sunrise.jpg"].read()
```

## CLI reference

| Command | Purpose |
| --- | --- |
| `itar create <archive>.itar` | Indexes `<archive>.tar` (single) or `<archive>-NN.tar` shards. |
| `itar ls <archive>.itar` | Lists members. Use `-l` for shard/offset info and `-H` for human-readable sizes. |
| `itar check <archive>.itar` | Validates every recorded entry and exits non-zero on corruption. |
