---
icon: lucide/terminal
---

# CLI

`itar` ships a small CLI for building and inspecting index files.

## Commands

| Command | Purpose |
| --- | --- |
| `itar index create <archive>.itar [--single TAR \| --shards shard0.tar shard1.tar ...]` | Build an index for a single archive or an explicit set of shards. With no flags, shards are auto-discovered next to `<archive>.itar`. |
| `itar index list <archive>.itar` | List members. Use `-l` for shard/offset info and `-H` for human-readable sizes. |
| `itar index check <archive>.itar` | Validate recorded entries; add `--member NAME` to focus on specific files. |
| `itar cat <archive>.itar <member>` | Stream a member’s bytes to stdout. |

## Shard naming

For sharded datasets, name shards with a zero-padded suffix before building the index:

```bash
tar cf photos-0.tar wedding/
tar cf photos-1.tar vacation/
itar index create photos.itar
```

The index is saved as `photos.itar`, alongside `photos-0.tar`, `photos-1.tar`, etc.
