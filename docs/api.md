---
icon: lucide/code-2
---

# Python API

`itar.open` returns an `IndexedTarFile`, a mapping-like view that streams members directly from the underlying tar shards. Use it as a context manager and access members like dictionary values.

::: itar
    options:
        show_root_heading: true

These helpers are the public surface for building and loading `.itar` indexes.

`itar.index.build` and `itar.index.create` accept a single TAR, an ordered list of
TARs, or a dictionary mapping integer shard IDs to TARs. Lists assign IDs starting
at zero; dictionaries preserve their keys. Pass the same list or dictionary to
`itar.open(..., shards=...)` when using custom filenames. Otherwise, filenames must
follow the [shard naming convention](cli.md#shard-naming).

::: itar.index
    options:
        show_root_heading: true
        filters:
            - "!^IndexLayout"
            - "!^DefaultResolver"
        separate_signature: true
        show_signature_annotations: true
