---
icon: lucide/code-2
---

# Python API

`itar.open` returns an `IndexedTarFile`, a mapping-like view that streams members directly from the underlying tar shards. Use it as a context manager and access members like dictionary values.

For multiprocessing, open an archive inside each worker; archive instances are
not pickleable.

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

## PyTorch DataLoader

Store the index path and member names in your dataset, then open the archive on
the first access in each worker. Each worker reuses its archive for later samples.

```python
import itar
from PIL import Image
from torch.utils.data import DataLoader, Dataset
from torchvision import transforms


class TarDataset(Dataset):
    def __init__(self, index_path, members):
        self.index_path = index_path
        self.members = members
        self.archive = None
        self.transform = transforms.Compose([
            transforms.Resize((224, 224)),
            transforms.ToTensor(),
        ])

    def __len__(self):
        return len(self.members)

    def __getitem__(self, index):
        if self.archive is None:
            self.archive = itar.open(self.index_path)
        with Image.open(self.archive[self.members[index]]) as image:
            return self.transform(image.convert("RGB"))


if __name__ == "__main__":
    dataset = TarDataset("photos.itar", ["wedding/cake.jpg", "vacation/sunrise.jpg"])
    loader = DataLoader(dataset, batch_size=2, num_workers=4, persistent_workers=True)
    for batch in loader:
        print(batch.shape)
```
