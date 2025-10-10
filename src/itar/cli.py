import argparse
import sys
from pathlib import Path

from . import index
from .utils import TarIndexError


def main() -> None:
    parser = argparse.ArgumentParser(description="Work with indexed TAR files.")
    subparsers = parser.add_subparsers(dest="command")
    subparsers.required = True

    cat_parser = subparsers.add_parser(
        "cat", help="Stream a member from an index to stdout"
    )
    cat_parser.add_argument("index", type=Path, help="Path to the .itar index file")
    cat_parser.add_argument("member", help="Member path to stream")
    cat_parser.set_defaults(func=_cmd_cat)

    ls_parser = subparsers.add_parser(
        "ls", aliases=["list"], help="List members recorded in an index"
    )
    ls_parser.add_argument("index", type=Path, help="Path to the .itar index file")
    ls_parser.add_argument(
        "-l", "--long", action="store_true", help="Show shard and offset details"
    )
    ls_parser.add_argument(
        "--bytes",
        action="store_true",
        help="Render sizes as number of bytes",
    )
    ls_parser.set_defaults(func=_cmd_ls)

    index_parser = subparsers.add_parser("index", help="Manage .itar index files")
    index_subparsers = index_parser.add_subparsers(dest="index_command")
    index_subparsers.required = True

    create_parser = index_subparsers.add_parser(
        "create", help="Create an index file from one or more TAR shards"
    )
    create_parser.add_argument("index", type=Path, help="Destination .itar index file")
    create_parser.add_argument(
        "--shards",
        type=Path,
        nargs="+",
        metavar="PATH",
        help=(
            "Explicit shard files to index. Use when you want to bypass auto-discovery."
        ),
    )
    create_parser.add_argument(
        "--single",
        type=Path,
        dest="single_tar",
        metavar="TAR",
        help="Path to a single TAR archive. Ignored if --shards is provided.",
    )
    create_parser.add_argument(
        "--no-progress",
        dest="progress",
        action="store_false",
        help="Disable the indexing progress bar",
    )
    create_parser.set_defaults(progress=True, func=_cmd_index_create)

    check_parser = index_subparsers.add_parser(
        "check", help="Verify every member recorded in an index file"
    )
    check_parser.add_argument("index", type=Path, help="Path to the .itar index file")
    check_parser.add_argument(
        "--member",
        dest="members",
        action="append",
        help="Limit verification to specific members (repeatable)",
    )
    check_parser.set_defaults(func=_cmd_index_check)

    list_parser = index_subparsers.add_parser(
        "list", aliases=["ls"], help="List members recorded in an index file"
    )
    list_parser.add_argument("index", type=Path, help="Path to the .itar index file")
    list_parser.add_argument(
        "-l", "--long", action="store_true", help="Show shard and offset details"
    )
    list_parser.add_argument(
        "--bytes",
        action="store_true",
        help="Render sizes as number of bytes",
    )
    list_parser.set_defaults(func=_cmd_index_list)

    args = parser.parse_args()
    try:
        args.func(args)
    except CLIError as exc:
        print(exc, file=sys.stderr)
        sys.exit(1)


class CLIError(RuntimeError):
    """Raised for recoverable CLI errors."""


def _resolve_shards_for_create(
    index_path: Path,
    explicit_shards: list[Path] | None,
    single_tar: Path | None,
):
    if explicit_shards:
        normalized = [Path(s) for s in explicit_shards]
        return normalized, len(normalized)

    if single_tar is not None:
        single_tar = Path(single_tar)
        if not single_tar.is_file():
            raise CLIError(f"Single tar not found: {single_tar}")
        return single_tar, None

    layout = index.IndexLayout(index_path)
    indexed_shards = layout.discover_shards()
    single_tar_candidate = layout.single_tar()
    has_single = single_tar_candidate.is_file()
    has_indexed = len(indexed_shards) > 0

    if has_single and has_indexed:
        raise CLIError(
            "Found both single (`name.tar`) and sharded (`name-NN.tar`) archives. "
            "Please remove one convention before building the index."
        )

    if has_single:
        shards = single_tar_candidate
        num_shards = None
    else:
        shards = indexed_shards
        num_shards = len(shards)
        if num_shards < 1:
            raise CLIError(
                f"No shards found for {index_path}.\n"
                f"Please create shard files first. Expected pattern: "
                f"'{index_path.stem}-NN.tar' (zero-padded shard index starting from 0) "
                f"or '{index_path.stem}.tar' for a single shard."
            )

        expected = layout.shards(num_shards)
        assert shards == expected, (
            f"Shards do not match expected names: {shards} != {expected}"
        )

    return shards, num_shards


def _cmd_cat(args) -> None:
    try:
        with index.open(args.index) as archive:
            try:
                source = archive.file(args.member)
            except KeyError as exc:  # pragma: no cover - defensive
                raise CLIError(f"Member not found: {args.member}") from exc

            try:
                while True:
                    chunk = source.read(64 * 1024)
                    if not chunk:
                        break
                    sys.stdout.buffer.write(chunk)
                sys.stdout.buffer.flush()
            finally:
                source.close()
    except FileNotFoundError as exc:  # pragma: no cover - defensive
        raise CLIError(str(exc)) from exc


def _cmd_index_create(args) -> None:
    index_path = Path(args.index)
    shards, num_shards = _resolve_shards_for_create(
        index_path, args.shards, args.single_tar
    )
    index.create(index_path, shards, progress_bar=args.progress)
    print(
        f"Wrote index to {index_path} with {1 if num_shards is None else num_shards} shard(s)."
    )


def _cmd_index_check(args) -> None:
    from tqdm import tqdm

    did_error = False
    members: list[str] | None = args.members if args.members else None

    with index.open(args.index) as archive:
        iterator = members if members is not None else archive
        for member in tqdm(iterator, desc="Checking files", unit="file"):
            try:
                archive.check_tar_index([member])
            except TarIndexError as e:
                print(e)
                did_error = True

    if did_error:
        sys.exit(1)


def _cmd_ls(args) -> None:
    _, current_index = index.load(args.index)
    if args.long:
        max_size = 0
        lines = []
        for member, (shard_idx, (offset, offset_data, size)) in current_index.items():
            if not args.bytes:
                from humanize import naturalsize

                size = naturalsize(size, gnu=True)
            else:
                size = str(size)
            max_size = max(max_size, len(size))
            lines.append((size, member))
        for line in [f"{size:>{max_size}} {member}" for size, member in lines]:
            print(line)
    else:
        for member in current_index:
            print(member)


def _cmd_index_list(args) -> None:
    _, current_index = index.load(args.index)
    if args.long:
        for member, (shard_idx, (offset, offset_data, size)) in current_index.items():
            if not args.bytes:
                from humanize import naturalsize

                size = naturalsize(size, gnu=True)
            print(
                f"{member:<40} {shard_idx:>5} {offset:>12} {offset_data:>12} {size:>10}"
            )
        print(f"{'NAME':<40} {'SHARD':>5} {'OFFSET':>12} {'OFF_DATA':>12} {'SIZE':>10}")
    else:
        for member in current_index:
            print(member)
