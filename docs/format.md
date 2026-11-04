---
icon: lucide/file-text
---

# File format

An `.itar` file is a MessagePack-encoded dictionary mapping member paths to metadata:

```python
{
    "path/to/member1.jpg": [
        null,  # integer shard ID or null for single archives
        [
            2048,  # metadata byte offset
            2560,  # data byte offset
            1048576,  # file length in bytes
        ],
    ],
    ...
}
```

- Sharded archives store the integer shard ID in the first position.
- Entries are recorded for files and links; directories are omitted.
- Offsets let `itar` serve members directly from the underlying tar file(s) without extraction.

Shard filenames follow the [shard naming convention](cli.md#shard-naming).

For duplicate member names, the last entry within a TAR or the last processed shard
wins. Autodiscovery processes shards in ascending numeric ID order; explicit lists
and dictionaries use their iteration order.
