# itar

[![PyPI version](https://img.shields.io/pypi/v/itar.svg)](https://pypi.python.org/pypi/itar)
[![docs](https://img.shields.io/badge/docs-brightgreen.svg)](https://kabouzeid.github.io/itar/)

`itar` builds constant‑time indexes over one or more tar file shards, enabling direct, random access to members without extracting the archives. It ships a lightweight CLI (`itar`) and a Python API.

Designed for large datasets and deep‑learning pipelines, it supports single or sharded tar archives with thread‑safe access for concurrent reads.

## Quickstart

```bash
pip install itar
```

### Single tarball

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
```

### Sharded tarballs

Give each shard a zero-padded suffix before building the index:

```bash
tar cf photos-0.tar wedding/    # shard 0
tar cf photos-1.tar vacation/   # shard 1

itar index create photos.itar   # discovers photos-0.tar, photos-1.tar, ...
itar index list -l photos.itar  # shard index, offsets, byte sizes
```

```python
import itar

with itar.open("photos.itar") as photos:
    assert "wedding/cake.jpg" in photos
    img_bytes = photos["vacation/sunrise.jpg"].read()
```

## Docs

Full CLI, API, and format details live in the [documentation site](https://kabouzeid.github.io/itar).
