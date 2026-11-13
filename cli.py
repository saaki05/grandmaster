import argparse

from lsmkv.store import KVStore


def main():
    parser = argparse.ArgumentParser(description="LSM-tree key-value store CLI")
    parser.add_argument("--data-dir", default="./data", help="Directory to store data files")
    sub = parser.add_subparsers(dest="command", required=True)

    put_p = sub.add_parser("put", help="Set a key")
    put_p.add_argument("key")
    put_p.add_argument("value")

    get_p = sub.add_parser("get", help="Get a key")
    get_p.add_argument("key")

    del_p = sub.add_parser("delete", help="Delete a key")
    del_p.add_argument("key")

    sub.add_parser("flush", help="Force-flush the memtable to a new SSTable")
    sub.add_parser("compact", help="Compact all SSTables into one")
    sub.add_parser("stats", help="Show store statistics")

    args = parser.parse_args()
    store = KVStore(args.data_dir)

    if args.command == "put":
        store.put(args.key, args.value)
        print(f"OK: {args.key} = {args.value}")
    elif args.command == "get":
        value = store.get(args.key)
        print(value if value is not None else "(not found)")
    elif args.command == "delete":
        store.delete(args.key)
        print(f"OK: deleted {args.key}")
    elif args.command == "flush":
        table = store.flush()
        print(f"Flushed to {table.base_path}" if table else "Nothing to flush")
    elif args.command == "compact":
        store.compact()
        print("Compaction complete")
    elif args.command == "stats":
        print(f"Memtable size: {len(store.memtable)}")
        print(f"SSTables on disk: {len(store.sstables)}")

    store.close()


if __name__ == "__main__":
    main()
