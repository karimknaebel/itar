import io
import os
import subprocess
import tarfile
import threading
from functools import partial

import msgpack
import pytest
from itar.sharded_indexed_tar import ShardedIndexedTar
from itar.utils import TarIndexError, build_tar_index


def make_tar_bytes(files):
    """Create an in-memory tarfile with given files (dict of name: bytes)."""
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w") as tf:
        for name, data in files.items():
            info = tarfile.TarInfo(name)
            info.size = len(data)
            tf.addfile(info, io.BytesIO(data))
    buf.seek(0)
    return buf


def make_sharded_tar_bytes(files):
    return [make_tar_bytes(f) for f in files]


@pytest.fixture
def sharded_tar_and_files(tmp_path):
    files = [
        {
            "a.txt": b"hello",
            "b.txt": b"world",
            "dir/c.txt": b"!",
        },
        {
            "c.txt": b"foo",
            "d.txt": b"bar",
            "dir/e.txt": b"baz",
        },
        {
            "f.txt": b"foo_f",
            "g.txt": b"bar_g",
            "dir2/h.txt": b"baz_h",
        },
    ]
    tar_bytes = make_sharded_tar_bytes(files)
    return tar_bytes, files


def test_sharded_indexed_tar_basic(sharded_tar_and_files):
    tar_bytes, files_ls = sharded_tar_and_files
    itar = ShardedIndexedTar(tar_bytes)
    assert set(itar.keys()) == set(f for files in files_ls for f in files)
    assert len(itar) == sum(len(files) for files in files_ls)
    for files in files_ls:
        for name, content in files.items():
            with itar[name] as f:
                assert f.read() == content
            assert name in itar
    assert list(itar) == sum((list(files) for files in files_ls), start=[])
    assert dict(itar.items()).keys() == set(f for files in files_ls for f in files)
    itar.close()


def test_sharded_indexed_tar_symlinks_and_hardlinks():
    # Create two shards: one with a file, one with symlink and hardlink
    files1 = {
        "file1.txt": b"data1",
    }

    # Create tarfile for files1
    buf1 = io.BytesIO()
    with tarfile.open(fileobj=buf1, mode="w") as tf:
        for name, data in files1.items():
            info = tarfile.TarInfo(name)
            info.size = len(data)
            tf.addfile(info, io.BytesIO(data))
    buf1.seek(0)

    # Create tarfile for files2 with symlink and hardlink
    buf2 = io.BytesIO()
    with tarfile.open(fileobj=buf2, mode="w") as tf:
        # Symlink in same shard
        symlink_info = tarfile.TarInfo("link1.txt")
        symlink_info.type = tarfile.SYMTYPE
        symlink_info.linkname = "file1.txt"
        tf.addfile(symlink_info)

        # Hardlink in same shard
        hardlink_info = tarfile.TarInfo("hard1.txt")
        hardlink_info.type = tarfile.LNKTYPE
        hardlink_info.linkname = "file1.txt"
        tf.addfile(hardlink_info)

        # Symlink to file in another shard (should not resolve, but test behavior)
        symlink_cross_info = tarfile.TarInfo("crosslink.txt")
        symlink_cross_info.type = tarfile.SYMTYPE
        symlink_cross_info.linkname = "../file1.txt"
        tf.addfile(symlink_cross_info)

    buf2.seek(0)

    # Compose shards
    tar_bytes = [buf1, buf2]
    itar = ShardedIndexedTar(tar_bytes)

    # file1.txt should be readable
    with itar["file1.txt"] as f:
        assert f.read() == b"data1"

    # link1.txt is a symlink to file1.txt (in another shard)
    with itar["link1.txt"] as f:
        assert f.read() == b"data1"

    # hard1.txt is a hardlink to file1.txt (in another shard)
    with itar["hard1.txt"] as f:
        assert f.read() == b"data1"

    # crosslink.txt is a symlink to ../file1.txt (should fail or be empty)
    with pytest.raises(KeyError):
        _ = itar["crosslink.txt"].read()

    itar.close()


def test_sharded_indexed_tar_symlinks_and_hardlinks_real_files(tmp_path):
    # Create a file in shard1
    file1 = tmp_path / "file1.txt"
    file1.write_bytes(b"data1")

    # Also create a regular file in shard2
    (tmp_path / "bar").mkdir()
    file2 = tmp_path / "bar" / "file2.txt"
    file2.write_bytes(b"data2")

    # Create a symlink in shard2 pointing to file1 in shard1
    symlink_path1 = tmp_path / "link1.txt"
    symlink_path1.symlink_to(
        os.path.relpath(file1, symlink_path1.parent)
    )  # symlink to file in another shard

    # Create a symlink in shard2 pointing to file1 in shard1
    (tmp_path / "foo").mkdir()
    symlink_path2 = tmp_path / "foo" / "link2.txt"
    symlink_path2.symlink_to(
        os.path.relpath(file2, symlink_path2.parent)
    )  # symlink to file in another shard

    # Create tar archives for each shard
    tar1_path = tmp_path / "shard1.tar"
    tar2_path = tmp_path / "shard2.tar"
    with tarfile.open(tar1_path, "w") as tf:
        tf.add(file1, arcname=file1.relative_to(tmp_path))
        tf.add(file2, arcname=file2.relative_to(tmp_path))
    with tarfile.open(tar2_path, "w") as tf:
        tf.add(symlink_path1, symlink_path1.relative_to(tmp_path))
        tf.add(symlink_path2, symlink_path2.relative_to(tmp_path))

    # Open as sharded archive
    itar = ShardedIndexedTar([tar1_path, tar2_path])

    assert itar.index["link1.txt"][1][2] == "file1.txt"  # symlink to file1.txt
    assert itar.index["foo/link2.txt"][1][2] == "bar/file2.txt"  # symlink to file2.txt

    # file1.txt should be readable
    with itar["file1.txt"] as f:
        assert f.read() == b"data1"

    # file2.txt is a regular file in shard2
    with itar["bar/file2.txt"] as f:
        assert f.read() == b"data2"

    # link1.txt is a symlink to file1.txt (in another shard)
    with itar["link1.txt"] as f:
        assert f.read() == b"data1"

    # link2.txt is a symlink to file2.txt (in another shard)
    with itar["foo/link2.txt"] as f:
        assert f.read() == b"data2"

    itar.close()


def test_sharded_indexed_tar_info(sharded_tar_and_files):
    tar_bytes, files_ls = sharded_tar_and_files
    itar = ShardedIndexedTar(tar_bytes)
    for files in files_ls:
        for name in files:
            info = itar.info(name)
            assert info.name == name
            assert info.size == len(files[name])
    itar.close()


def test_sharded_indexed_tar_verify_index(sharded_tar_and_files):
    tar_bytes, files_ls = sharded_tar_and_files
    itar = ShardedIndexedTar(tar_bytes)
    itar.check_tar_index()
    itar.close()


def test_sharded_indexed_tar_verify_index_raises(sharded_tar_and_files):
    tar_bytes, files_ls = sharded_tar_and_files
    itar = ShardedIndexedTar(
        tar_bytes,
        index={
            k: (i, (0, 512, 0))
            for i, files in enumerate(files_ls)
            for k, v in files.items()
        },
    )
    with pytest.raises(TarIndexError):
        itar.check_tar_index()
    itar.close()


def test_sharded_indexed_tar_context_manager(sharded_tar_and_files):
    tar_bytes, files_ls = sharded_tar_and_files
    with ShardedIndexedTar(tar_bytes) as itar:
        for files in files_ls:
            for name in files:
                assert itar[name].read() == files[name]


def test_sharded_indexed_tar_build_index(tmp_path):
    files_ls = [
        {"x.txt": b"some", "y.txt": b"files"},
        {"a.txt": b"foo", "b.txt": b"bar"},
    ]
    tar_bytes = make_sharded_tar_bytes(files_ls)
    itar = ShardedIndexedTar(tar_bytes)
    assert itar.index == {
        "x.txt": (0, (0, 512, 4)),
        "y.txt": (0, (1024, 1536, 5)),
        "a.txt": (1, (0, 512, 3)),
        "b.txt": (1, (1024, 1536, 3)),
    }
    assert set(itar.keys()) == set(f for files in files_ls for f in files)
    for files in files_ls:
        for name in files:
            assert itar[name].read() == files[name]
    itar.close()


def test_indexed_tar_missing_key(sharded_tar_and_files):
    tar_bytes, _ = sharded_tar_and_files
    itar = ShardedIndexedTar(tar_bytes)
    with pytest.raises(KeyError):
        _ = itar["notfound.txt"]
    itar.close()


@pytest.fixture
def gnu_sparse_tar(tmp_path):
    # Create a sparse file
    sparse_file = tmp_path / "sparsefile"
    with open(sparse_file, "wb") as f:
        f.write(b"START")  # Write something at the start
        f.seek(1024 * 1024 * 10)  # 10 MiB hole
        f.write(b"END")  # Write something at the end

    tar_path = tmp_path / "sparse.tar"

    # Create GNU sparse tar using the system tar command
    subprocess.run(
        ["tar", "--sparse", "--format=gnu", "-cf", str(tar_path), "sparsefile"],
        cwd=tmp_path,
        check=True,
    )

    return tar_path


def test_threadlocalpreadio_vs_open_with_sparse(gnu_sparse_tar):
    with pytest.raises(NotImplementedError):
        build_tar_index(gnu_sparse_tar)


def test_sharded_indexed_tar_race_condition(tmp_path):
    """
    This test demonstrates a race condition in the non-thread-safe ShardedIndexedTar
    when multiple threads read from the same shard. The file descriptor offset is shared,
    so concurrent reads can interfere with each other, causing data corruption.

    This test can be flaky, as it relies on timing and may not always trigger.
    """
    NUM_READS = 10000  # increase to make it more likely to hit

    files = {"a.txt": b"hello", "b.txt": b"world"}
    tar_path = tmp_path / "race_shard-0.tar"
    with tarfile.open(tar_path, "w") as tf:
        for name, data in files.items():
            info = tarfile.TarInfo(name)
            info.size = len(data)
            tf.addfile(info, io.BytesIO(data))

    itar_path = tmp_path / "race_shard.itar"
    with ShardedIndexedTar([tar_path]) as itar:
        itar.save(itar_path)

    def multi_threaded_read_corrupted(itar):
        results = {}

        def read_file(name):
            # Read the file multiple times to increase the chance of a race
            for _ in range(NUM_READS):
                with itar[name] as f:
                    data = f.read()
                    if data != files[name]:
                        # Save the corrupted data for debugging
                        results.setdefault(name, []).append(data)

        threads = []
        for name in files:
            t = threading.Thread(target=read_file, args=(name,))
            threads.append(t)
            t.start()
        for t in threads:
            t.join()

        # If the implementation is not thread-safe, at least one read should be corrupted
        return any(
            any(d != files[name] for d in datas) for name, datas in results.items()
        )

    # not thread-safe, should have corrupted reads
    with ShardedIndexedTar.open(
        itar_path, open_fn=partial(open, mode="rb", buffering=0)
    ) as itar:
        assert multi_threaded_read_corrupted(itar), (
            "There should be corrupted reads due to race conditions"
        )

    # thread-safe, should not have corrupted reads
    with ShardedIndexedTar.open(itar_path) as itar:
        assert not multi_threaded_read_corrupted(itar), (
            "There should be no corrupted reads in thread-safe mode"
        )


def test_sharded_indexed_tar_open_and_save(tmp_path, sharded_tar_and_files):
    tar_bytes, files_ls = sharded_tar_and_files

    # Save the shards to disk to test open/save with file paths
    shard_paths = []
    for i, buf in enumerate(tar_bytes):
        path = tmp_path / f"archive-{i:0{len(str(len(tar_bytes) - 1))}d}.tar"
        with open(path, "wb") as f:
            f.write(buf.getbuffer())
        shard_paths.append(str(path))

    # Create ShardedIndexedTar and save the index
    itar_path = tmp_path / "archive.itar"
    with ShardedIndexedTar(shard_paths) as itar:
        itar.save(itar_path)

    # Check that the saved file exists and is a valid msgpack file
    with open(itar_path, "rb") as f:
        num_shards, index = msgpack.load(f)
    assert num_shards == len(shard_paths)
    assert isinstance(index, dict)
    assert set(index.keys()) == set(f for files in files_ls for f in files)

    # Open the archive using the classmethod and check contents
    itar2 = ShardedIndexedTar.open(itar_path)
    assert set(itar2.keys()) == set(f for files in files_ls for f in files)
    for files in files_ls:
        for name, content in files.items():
            with itar2[name] as f:
                assert f.read() == content
    itar2.close()

    # Open the archive with explicit shards argument
    itar3 = ShardedIndexedTar.open(itar_path, shards=shard_paths)
    assert set(itar3.keys()) == set(f for files in files_ls for f in files)
    itar3.close()


def test_single_shard_basename_roundtrip(tmp_path):
    files = {"foo.txt": b"hello", "dir/bar.txt": b"world"}
    tar_buf = make_tar_bytes(files)

    tar_path = tmp_path / "archive.tar"
    with open(tar_path, "wb") as f:
        f.write(tar_buf.getbuffer())

    itar_path = tmp_path / "archive.itar"
    with ShardedIndexedTar(tar_path) as itar:
        itar.save(itar_path)

    with open(itar_path, "rb") as f:
        num_shards, index = msgpack.load(f)
    assert num_shards is None
    assert set(index.keys()) == set(files.keys())

    with ShardedIndexedTar.open(itar_path) as reopened:
        for name, content in files.items():
            with reopened[name] as fh:
                assert fh.read() == content
