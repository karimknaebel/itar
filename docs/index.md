---
icon: lucide/archive
---

# itar

Constant-time indexes for single or sharded tar archives. Build an index once, then read members directly without extracting the tarballs.

## Quickstart

Install the CLI (includes the Python API):

```bash
pip install itar[cli]
```

### Single tarball

```bash
echo "Hello world!" > hello.txt
tar cf hello.tar hello.txt

itar index create hello.itar
itar index list hello.itar
```

```python
import itar

with itar.open("hello.itar") as archive:
    print(archive["hello.txt"].read())
```

### Sharded tarballs

```bash
tar cf photos-0.tar wedding/
tar cf photos-1.tar vacation/

itar index create photos.itar   # discovers shards automatically
itar index list -l photos.itar
```

```python
import itar

with itar.open("photos.itar") as photos:
    assert "wedding/cake.jpg" in photos
    img_bytes = photos["vacation/sunrise.jpg"].read()
```

## What’s inside

- The **CLI** page covers `itar index` and `itar cat`.
- The **Python API** page documents the helper functions used above.
- The **File Format** page explains the MessagePack index layout.
