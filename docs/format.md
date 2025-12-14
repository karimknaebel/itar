---
icon: lucide/file-text
---

# File format

An `.itar` file is a MessagePack-encoded dictionary mapping member paths to metadata:

```python
{
    "path/to/member1.jpg": [
        null,  # shard index (0-based) or null for single archives
        [
            2048,  # metadata byte offset
            2560,  # data byte offset
            1048576,  # file length in bytes
        ],
    ],
    ...
}
```

- Sharded archives store the shard index in the first position.
- Entries are recorded for files and links; directories are omitted.
- Offsets let `itar` serve members directly from the underlying tar file(s) without extraction.
