# itar

## Minimal example

```bash
echo Hello world! > bar.txt
tar cf foo-0.tar bar.txt  # create regular tar file(s) with zero-padded shard number
itar create foo.itar  # create the index
itar ls foo.itar  # (optional) view the contents of the index
```

Now you can open and efficiently access the archived files in python in constant time
```python
from mini.itar import ShardedIndexedTar

with ShardedIndexedTar.open("foo.itar") as itar:
    f = itar.file("bar.txt")  # file-like object
    assert f.read().decode("utf-8") == "Hello world!\n"
```
