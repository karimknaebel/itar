import io
import os
import subprocess
import tarfile
import threading
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

import pytest

import itar
from itar.cli import CLIError, _cmd_cat, _cmd_index_create, main
from itar.indexed_tar_file import IndexedTarFile
from itar.utils import TarIndexError, ThreadSafeFileIO, build_tar_index


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


def make_indexed_tar(shards, return_index=False, **kwargs):
    index_data = itar.index.build(shards)
    tf = IndexedTarFile(shards, index_data, **kwargs)
    return (tf, index_data) if return_index else tf


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


def test_indexed_tar_file_basic(sharded_tar_and_files):
    tar_bytes, files_ls = sharded_tar_and_files
    archive = make_indexed_tar(tar_bytes)
    assert set(archive.keys()) == set(f for files in files_ls for f in files)
    assert len(archive) == sum(len(files) for files in files_ls)
    for files in files_ls:
        for name, content in files.items():
            with archive[name] as f:
                assert f.read() == content
            assert name in archive
    assert list(archive) == sum((list(files) for files in files_ls), start=[])
    assert dict(archive.items()).keys() == set(f for files in files_ls for f in files)
    archive.close()


def test_indexed_tar_file_symlinks_and_hardlinks():
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
    archive = make_indexed_tar(tar_bytes)

    # file1.txt should be readable
    with archive["file1.txt"] as f:
        assert f.read() == b"data1"

    # link1.txt is a symlink to file1.txt (in another shard)
    with archive["link1.txt"] as f:
        assert f.read() == b"data1"

    # hard1.txt is a hardlink to file1.txt (in another shard)
    with archive["hard1.txt"] as f:
        assert f.read() == b"data1"

    # crosslink.txt is a symlink to ../file1.txt (should fail or be empty)
    with pytest.raises(KeyError):
        _ = archive["crosslink.txt"].read()

    archive.close()


def test_indexed_tar_file_symlinks_and_hardlinks_real_files(tmp_path):
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
    archive, index = make_indexed_tar([tar1_path, tar2_path], return_index=True)

    assert index["link1.txt"][1][2] == "file1.txt"  # symlink to file1.txt
    assert index["foo/link2.txt"][1][2] == "bar/file2.txt"  # symlink to file2.txt

    # file1.txt should be readable
    with archive["file1.txt"] as f:
        assert f.read() == b"data1"

    # file2.txt is a regular file in shard2
    with archive["bar/file2.txt"] as f:
        assert f.read() == b"data2"

    # link1.txt is a symlink to file1.txt (in another shard)
    with archive["link1.txt"] as f:
        assert f.read() == b"data1"

    # link2.txt is a symlink to file2.txt (in another shard)
    with archive["foo/link2.txt"] as f:
        assert f.read() == b"data2"

    archive.close()


def test_indexed_tar_file_info(sharded_tar_and_files):
    tar_bytes, files_ls = sharded_tar_and_files
    archive = make_indexed_tar(tar_bytes)
    for files in files_ls:
        for name in files:
            info = archive.info(name)
            assert info.name == name
            assert info.size == len(files[name])
    archive.close()


def test_indexed_tar_file_verify_index(sharded_tar_and_files):
    tar_bytes, files_ls = sharded_tar_and_files
    archive = make_indexed_tar(tar_bytes)
    archive.check_tar_index()
    archive.close()


def test_indexed_tar_file_verify_index_raises(sharded_tar_and_files):
    tar_bytes, files_ls = sharded_tar_and_files
    archive = IndexedTarFile(
        tar_bytes,
        {
            k: (i, (0, 512, 0))
            for i, files in enumerate(files_ls)
            for k, v in files.items()
        },
    )
    with pytest.raises(TarIndexError):
        archive.check_tar_index()
    archive.close()


def test_indexed_tar_file_context_manager(sharded_tar_and_files):
    tar_bytes, files_ls = sharded_tar_and_files
    with make_indexed_tar(tar_bytes) as archive:
        for files in files_ls:
            for name in files:
                assert archive[name].read() == files[name]


def test_indexed_tar_file_build_index(tmp_path):
    files_ls = [
        {"x.txt": b"some", "y.txt": b"files"},
        {"a.txt": b"foo", "b.txt": b"bar"},
    ]
    tar_bytes = make_sharded_tar_bytes(files_ls)
    archive, index = make_indexed_tar(tar_bytes, return_index=True)
    assert index == {
        "x.txt": (0, (0, 512, 4)),
        "y.txt": (0, (1024, 1536, 5)),
        "a.txt": (1, (0, 512, 3)),
        "b.txt": (1, (1024, 1536, 3)),
    }
    assert set(archive.keys()) == set(f for files in files_ls for f in files)
    for files in files_ls:
        for name in files:
            assert archive[name].read() == files[name]
    archive.close()


def test_indexed_tar_missing_key(sharded_tar_and_files):
    tar_bytes, _ = sharded_tar_and_files
    archive = make_indexed_tar(tar_bytes)
    with pytest.raises(KeyError):
        _ = archive["notfound.txt"]
    archive.close()


def test_cli_cat_streams_member(tmp_path, capsys):
    files = {"foo.txt": b"hello"}
    tar_buf = make_tar_bytes(files)
    tar_path = tmp_path / "cat.tar"
    tar_path.write_bytes(tar_buf.getbuffer())

    index_path = tmp_path / "cat.itar"
    itar.index.create(index_path, tar_path)

    _cmd_cat(SimpleNamespace(index=index_path, member="foo.txt"))
    captured = capsys.readouterr()
    assert captured.out == "hello"


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


@pytest.mark.parametrize(
    "method, args",
    [
        ("read", ()),
        ("read", (0,)),
        ("read", (1,)),
        ("seek", (0,)),
        ("seek", (0, os.SEEK_END)),
        ("tell", ()),
        ("fileno", ()),
        ("readable", ()),
        ("seekable", ()),
        ("writable", ()),
        ("__enter__", ()),
    ],
)
def test_threadsafe_file_rejects_io_after_close(tmp_path, method, args):
    path = tmp_path / "file"
    path.write_bytes(b"hello")
    reader = ThreadSafeFileIO(path)
    reader.close()
    with pytest.raises(ValueError, match="closed file"):
        getattr(reader, method)(*args)
    reader.close()


@pytest.mark.parametrize("buffered", [False, True])
def test_member_read_after_archive_close(tmp_path, buffered):
    path = tmp_path / "archive.tar"
    path.write_bytes(make_tar_bytes({"a.txt": b"hello"}).getvalue())
    with IndexedTarFile(
        path, itar.index.build(path), buffered_file_reader=buffered
    ) as archive:
        member = archive["a.txt"]

    with member:
        with pytest.raises(ValueError, match="closed file"):
            member.read()


@pytest.mark.parametrize("reader_type", [io.FileIO, ThreadSafeFileIO])
@pytest.mark.parametrize("buffered", [False, True])
def test_indexed_tar_file_concurrent_reads(tmp_path, reader_type, buffered):
    files = {"a.txt": b"hello", "b.txt": b"world"}
    tar_path = tmp_path / "race_shard-0.tar"
    tar_path.write_bytes(make_tar_bytes(files).getvalue())
    index_path = tmp_path / "race_shard.itar"
    itar.index.create(index_path, [tar_path], progress_bar=False)
    barrier = threading.Barrier(2, timeout=10)

    class CoordinatedReader(reader_type):
        def seek(self, offset, whence=0):
            position = super().seek(offset, whence)
            barrier.wait()
            return position

    with itar.open(
        index_path, open_fn=CoordinatedReader, buffered_file_reader=buffered
    ) as archive:
        archive.file("a.txt").close()

        def read_file(name):
            with archive[name] as member:
                return member.read()

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(read_file, files))

    if reader_type is ThreadSafeFileIO:
        assert results == list(files.values())
    else:
        assert results != list(files.values())


def test_concurrent_first_access_opens_shard_once(tmp_path):
    files = {"a.txt": b"hello", "b.txt": b"world"}
    tar_path = tmp_path / "archive-0.tar"
    tar_path.write_bytes(make_tar_bytes(files).getvalue())
    index_path = tmp_path / "archive.itar"
    itar.index.create(index_path, [tar_path], progress_bar=False)
    barrier = threading.Barrier(2, timeout=10)
    local = threading.local()
    handles = []

    class CoordinatedHandles(dict):
        def __contains__(self, key):
            found = super().__contains__(key)
            if not found and not getattr(local, "checked", False):
                local.checked = True
                barrier.wait()
            return found

    def open_shard(path):
        handle = ThreadSafeFileIO(path)
        handles.append(handle)
        return handle

    with itar.open(index_path, open_fn=open_shard) as archive:
        archive._handles = CoordinatedHandles()
        with ThreadPoolExecutor(max_workers=2) as pool:
            members = list(pool.map(archive.file, files))
        for member, expected in zip(members, files.values(), strict=True):
            with member:
                assert member.read() == expected

    assert len(handles) == 1
    assert all(handle.closed for handle in handles)


def test_indexed_tar_file_open_and_save(tmp_path, sharded_tar_and_files):
    tar_bytes, files_ls = sharded_tar_and_files

    # Save the shards to disk to test open/save with file paths
    shard_paths = []
    for i, buf in enumerate(tar_bytes):
        path = tmp_path / f"archive-{i:0{len(str(len(tar_bytes) - 1))}d}.tar"
        with open(path, "wb") as f:
            f.write(buf.getbuffer())
        shard_paths.append(str(path))

    # Create an index file from helper functions
    index_path = tmp_path / "archive.itar"
    itar.index.create(index_path, shard_paths)

    # Check that the saved file exists and is a valid msgpack file
    index_mapping = itar.index.load(index_path)
    assert isinstance(index_mapping, dict)
    assert set(index_mapping.keys()) == set(f for files in files_ls for f in files)
    assert {shard_idx for shard_idx, _ in index_mapping.values()} == set(
        range(len(shard_paths))
    )

    # Open the archive using the classmethod and check contents
    archive2 = itar.open(index_path)
    assert set(archive2.keys()) == set(f for files in files_ls for f in files)
    for files in files_ls:
        for name, content in files.items():
            with archive2[name] as f:
                assert f.read() == content
    archive2.close()

    # Open the archive with explicit shards argument
    archive3 = itar.open(index_path, shards=shard_paths)
    assert set(archive3.keys()) == set(f for files in files_ls for f in files)
    archive3.close()


@pytest.mark.parametrize("missing_shard", [0, 1, 2])
def test_open_with_missing_shard(tmp_path, missing_shard):
    index_path = tmp_path / "archive.itar"
    shards = [tmp_path / f"archive-{i}.tar" for i in range(3)]
    for i, path in enumerate(shards):
        path.write_bytes(make_tar_bytes({f"{i}.txt": str(i).encode()}).getvalue())
    itar.index.create(index_path, shards, progress_bar=False)
    shards[missing_shard].unlink()

    with itar.open(index_path) as archive:
        with pytest.raises(FileNotFoundError, match=f"Shard {missing_shard}"):
            archive[f"{missing_shard}.txt"]
        for i in range(3):
            if i != missing_shard:
                with archive[f"{i}.txt"] as member:
                    assert member.read() == str(i).encode()


def test_default_resolver_uses_numeric_suffixes(tmp_path):
    layout = itar.index.IndexLayout(tmp_path / "my-archive.itar")
    for suffix in ["1", "02", "10"]:
        (tmp_path / f"my-archive-{suffix}.tar").touch()

    resolver = itar.index.DefaultResolver(layout)
    assert resolver(1) == tmp_path / "my-archive-1.tar"
    assert resolver(2) == tmp_path / "my-archive-02.tar"
    assert resolver(10) == tmp_path / "my-archive-10.tar"


def test_default_resolver_rejects_duplicate_shard_indices(tmp_path):
    (tmp_path / "archive-0.tar").touch()
    (tmp_path / "archive-00.tar").touch()
    resolver = itar.index.DefaultResolver(
        itar.index.IndexLayout(tmp_path / "archive.itar")
    )

    with pytest.raises(ValueError, match="Duplicate shard ID 0") as exc:
        resolver(0)
    assert "archive-0.tar" in str(exc.value)
    assert "archive-00.tar" in str(exc.value)


@pytest.mark.parametrize("command", [["index", "create"], ["index", "check"], ["cat"]])
def test_cli_rejects_duplicate_shard_ids(tmp_path, monkeypatch, capsys, command):
    index_path = tmp_path / "archive.itar"
    shard_path = tmp_path / "archive-0.tar"
    shard_path.write_bytes(make_tar_bytes({"foo.txt": b"foo"}).getvalue())
    itar.index.create(index_path, [shard_path], progress_bar=False)
    (tmp_path / "archive-00.tar").touch()
    monkeypatch.setattr(
        "sys.argv",
        ["itar", *command, str(index_path)]
        + (["foo.txt"] if command == ["cat"] else []),
    )

    with pytest.raises(SystemExit) as exc:
        main()
    assert exc.value.code == 1
    error = capsys.readouterr().err
    assert "Duplicate shard ID 0" in error
    assert "archive-0.tar" in error
    assert "archive-00.tar" in error


def test_discover_shards_matches_literal_stem_and_decimal_ids(tmp_path):
    for name in ["photos[1]-7.tar", "photos[1]-002.tar", "photos[1]-10.tar"]:
        (tmp_path / name).touch()
    for name in [
        "photos1-3.tar",
        "photos[1]--3.tar",
        "photos[1]-x.tar",
        "photos[1]-3.tar.gz",
    ]:
        (tmp_path / name).touch()
    (tmp_path / "photos[1]-4.tar").mkdir()

    shards = itar.index.IndexLayout(tmp_path / "photos[1].itar").discover_shards()
    assert list(shards) == [2, 7, 10]
    assert shards[2] == tmp_path / "photos[1]-002.tar"


def test_explicit_shard_ids_roundtrip(tmp_path):
    shards = {7: make_tar_bytes({"a.txt": b"a"}), 42: make_tar_bytes({"b.txt": b"b"})}
    index_path = tmp_path / "archive.itar"
    itar.index.create(index_path, shards, progress_bar=False)

    with itar.open(index_path, shards=shards) as archive:
        with archive["a.txt"] as member:
            assert member.read() == b"a"
        with archive["b.txt"] as member:
            assert member.read() == b"b"
    assert all(not shard.closed for shard in shards.values())


@pytest.mark.parametrize("failure", [None, "missing", "invalid"])
def test_build_closes_shards_sequentially(tmp_path, monkeypatch, failure):
    stream = make_tar_bytes({"stream.txt": b"stream"})
    first = tmp_path / "first.tar"
    first.write_bytes(make_tar_bytes({"first.txt": b"first"}).getvalue())
    second = tmp_path / "second.tar"
    if failure == "invalid":
        second.write_bytes(b"not a tar file")
    elif failure is None:
        second.write_bytes(make_tar_bytes({"second.txt": b"second"}).getvalue())

    handles = []
    original_open = open

    def tracked_open(*args, **kwargs):
        assert all(handle.closed for handle in handles)
        handle = original_open(*args, **kwargs)
        handles.append(handle)
        return handle

    monkeypatch.setattr("builtins.open", tracked_open)
    monkeypatch.setattr(tarfile, "bltn_open", tracked_open)
    shards = {7: stream, 10: first, 42: second}
    if failure is None:
        index = itar.index.build(shards)
        assert {name: record[0] for name, record in index.items()} == {
            "stream.txt": 7,
            "first.txt": 10,
            "second.txt": 42,
        }
    else:
        with pytest.raises(
            FileNotFoundError if failure == "missing" else tarfile.ReadError
        ):
            itar.index.build(shards)

    assert len(handles) == (1 if failure == "missing" else 2)
    assert all(handle.closed for handle in handles)
    assert not stream.closed


def test_build_leaves_invalid_stream_open():
    stream = io.BytesIO(b"not a tar file")
    with pytest.raises(tarfile.ReadError):
        itar.index.build(stream)
    assert not stream.closed


def test_explicit_shard_mapping_is_copied():
    shards = {7: make_tar_bytes({"a.txt": b"original"})}
    with IndexedTarFile(shards, itar.index.build(shards)) as archive:
        shards[7] = make_tar_bytes({"a.txt": b"replaced"})
        with archive["a.txt"] as member:
            assert member.read() == b"original"


@pytest.mark.parametrize("ambiguous", [False, True])
def test_create_rejects_missing_or_ambiguous_shards(tmp_path, ambiguous):
    if ambiguous:
        (tmp_path / "archive.tar").touch()
        (tmp_path / "archive-7.tar").touch()
    with pytest.raises(
        CLIError, match="Found both" if ambiguous else "No shards found"
    ):
        _cmd_index_create(
            SimpleNamespace(
                index=tmp_path / "archive.itar",
                shards=None,
                single_tar=None,
                progress=False,
            )
        )


def test_build_duplicate_members_use_last_entry():
    stream = io.BytesIO()
    with tarfile.open(fileobj=stream, mode="w") as archive:
        for data in [b"first", b"last"]:
            member = tarfile.TarInfo("duplicate.txt")
            member.size = len(data)
            archive.addfile(member, io.BytesIO(data))

    with IndexedTarFile(stream, itar.index.build(stream)) as archive:
        with archive["duplicate.txt"] as member:
            assert member.read() == b"last"


def test_create_duplicate_members_use_highest_discovered_shard_id(tmp_path):
    for shard_id in [10, 2]:
        (tmp_path / f"archive-{shard_id}.tar").write_bytes(
            make_tar_bytes({"duplicate.txt": str(shard_id).encode()}).getvalue()
        )
    index_path = tmp_path / "archive.itar"
    _cmd_index_create(
        SimpleNamespace(index=index_path, shards=None, single_tar=None, progress=False)
    )

    assert itar.index.load(index_path)["duplicate.txt"][0] == 10
    with itar.open(index_path) as archive:
        with archive["duplicate.txt"] as member:
            assert member.read() == b"10"


def test_single_shard_basename_roundtrip(tmp_path):
    files = {"foo.txt": b"hello", "dir/bar.txt": b"world"}
    tar_buf = make_tar_bytes(files)

    tar_path = tmp_path / "archive.tar"
    with open(tar_path, "wb") as f:
        f.write(tar_buf.getbuffer())

    index_path = tmp_path / "archive.itar"
    itar.index.create(index_path, tar_path)

    index_mapping = itar.index.load(index_path)
    assert set(index_mapping.keys()) == set(files.keys())
    assert {shard_idx for shard_idx, _ in index_mapping.values()} == {None}

    with itar.open(index_path) as reopened:
        index_path.unlink()
        for name, content in files.items():
            with reopened[name] as fh:
                assert fh.read() == content


def test_single_shard_with_suffix(tmp_path):
    files = {"foo.txt": b"foo"}
    shard_path = tmp_path / "archive-0.tar"
    with tarfile.open(shard_path, "w") as tf:
        for name, data in files.items():
            info = tarfile.TarInfo(name)
            info.size = len(data)
            tf.addfile(info, io.BytesIO(data))

    index_path = tmp_path / "archive.itar"
    itar.index.create(index_path, [shard_path])

    index_mapping = itar.index.load(index_path)
    assert {shard_idx for shard_idx, _ in index_mapping.values()} == {0}

    with itar.open(index_path) as reopened:
        with reopened["foo.txt"] as fh:
            assert fh.read() == b"foo"


def test_single_input_bytesio_dump(tmp_path):
    files = {"only.txt": b"payload"}
    tar_buf = make_tar_bytes(files)

    index_path = tmp_path / "single.itar"
    index = itar.index.build(tar_buf)
    itar.index.dump(index, index_path)

    index_mapping = itar.index.load(index_path)
    assert index_mapping == index
    assert set(index_mapping) == set(files)
    assert {shard_idx for shard_idx, _ in index_mapping.values()} == {None}


@pytest.mark.parametrize(
    "index",
    [
        {},
        {
            "dir/文件.txt": (None, (0, 512, 0)),
            "link": (0, (1024, 1536, "dir/文件.txt")),
            "large": (256, (2**63, 2**63 + 512, 2**64 - 1)),
        },
    ],
)
def test_index_roundtrip(tmp_path, index):
    index_path = tmp_path / "archive.itar"
    itar.index.dump(index, index_path)
    assert itar.index.load(index_path) == index


def test_open_with_explicit_shards_override(tmp_path):
    files = {"foo.txt": b"foo"}
    tar_path = tmp_path / "explicit.tar"
    with tarfile.open(tar_path, "w") as tf:
        for name, data in files.items():
            info = tarfile.TarInfo(name)
            info.size = len(data)
            tf.addfile(info, io.BytesIO(data))

    index_path = tmp_path / "explicit.itar"
    itar.index.create(index_path, tar_path)

    reopened = itar.open(index_path, shards=tar_path)
    try:
        assert set(reopened.keys()) == set(files)
        with reopened["foo.txt"] as f:
            assert f.read() == b"foo"
    finally:
        reopened.close()

    reopened = itar.open(index_path, shards=[tar_path])
    try:
        with pytest.raises(KeyError):
            reopened["foo.txt"]
    finally:
        reopened.close()


def test_create_detects_single_tar(tmp_path):
    files = {"foo.txt": b"foo"}
    tar_buf = make_tar_bytes(files)

    tar_path = tmp_path / "archive.tar"
    with open(tar_path, "wb") as f:
        f.write(tar_buf.getbuffer())

    index_path = tmp_path / "archive.itar"
    _cmd_index_create(
        SimpleNamespace(index=index_path, shards=None, single_tar=None, progress=True)
    )

    index_mapping = itar.index.load(index_path)
    assert set(index_mapping.keys()) == set(files)
    assert {shard_idx for shard_idx, _ in index_mapping.values()} == {None}

    with itar.open(index_path) as reopened:
        with reopened["foo.txt"] as fh:
            assert fh.read() == b"foo"


@pytest.mark.parametrize("suffixes", [("0", "1"), ("0007", "42"), ("10", "002")])
def test_create_detects_sharded(tmp_path, suffixes):
    files_a = {"foo.txt": b"foo"}
    files_b = {"bar.txt": b"bar"}
    shard_data = [files_a, files_b]

    for suffix, mapping in zip(suffixes, shard_data, strict=True):
        buf = make_tar_bytes(mapping)
        shard_path = tmp_path / f"archive-{suffix}.tar"
        with open(shard_path, "wb") as f:
            f.write(buf.getbuffer())

    index_path = tmp_path / "archive.itar"
    _cmd_index_create(
        SimpleNamespace(index=index_path, shards=None, single_tar=None, progress=True)
    )

    index_mapping = itar.index.load(index_path)
    assert index_mapping["foo.txt"][0] == int(suffixes[0])
    assert index_mapping["bar.txt"][0] == int(suffixes[1])
    assert set(index_mapping.keys()) == {"foo.txt", "bar.txt"}

    with itar.open(index_path) as reopened:
        assert set(reopened.keys()) == {"foo.txt", "bar.txt"}
        for mapping in shard_data:
            for name, data in mapping.items():
                with reopened[name] as member:
                    assert member.read() == data
