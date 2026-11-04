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

For `photos.itar`, place shards named `photos-<ID>.tar` in the same directory.
`ID` is a nonnegative decimal integer. Leading zeros do not affect its value:
`photos-7.tar` and `photos-0007.tar` both identify shard 7. IDs may have gaps and
need not start at zero. Each numeric ID must be unique; duplicate IDs are an error
when creating or reading an index through autodiscovery.

```bash
tar cf photos-0007.tar wedding/
tar cf photos-42.tar vacation/
itar index create photos.itar
```

The index records shard IDs 7 and 42. Missing shards do not change the IDs of the
remaining files; accessing a missing shard raises `FileNotFoundError`.

With `--shards`, filenames are unrestricted and shard IDs are assigned by list
position starting at zero. Reopen such an index with the same ordered shard list
using `itar.open(..., shards=[...])` unless its filenames already match those IDs
and the index basename.
