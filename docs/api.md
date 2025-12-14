---
icon: lucide/code-2
---

# Python API

`itar.open` returns an `IndexedTarFile`, a mapping-like view that streams members directly from the underlying tar shards. Use it as a context manager and access members like dictionary values.

::: itar
    options:
        show_root_heading: true

These helpers are the public surface for building and loading `.itar` indexes.

::: itar.index
    options:
        show_root_heading: true
        filters:
            - "!^IndexLayout"
            - "!^DefaultResolver"
        separate_signature: true
        show_signature_annotations: true
