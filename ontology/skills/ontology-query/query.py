from __future__ import annotations

import argparse
from pathlib import Path

from ontology_tools import class_json, classes_json, find_json, individuals_json, properties_json, sparql_json


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Query the Extend0 ontology.")
    parser.add_argument("--abox", action="append", default=[], help="Additional ABox Turtle file to load.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("classes")

    class_parser = subparsers.add_parser("class")
    class_parser.add_argument("name")

    subparsers.add_parser("properties")
    subparsers.add_parser("individuals")

    find_parser = subparsers.add_parser("find")
    find_parser.add_argument("term")

    sparql_parser = subparsers.add_parser("sparql")
    sparql_parser.add_argument("query")

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    abox_paths = [str(Path(path)) for path in args.abox]

    if args.command == "classes":
        print(classes_json(abox_paths))
    elif args.command == "class":
        print(class_json(args.name, abox_paths))
    elif args.command == "properties":
        print(properties_json(abox_paths))
    elif args.command == "individuals":
        print(individuals_json(abox_paths))
    elif args.command == "find":
        print(find_json(args.term, abox_paths))
    elif args.command == "sparql":
        print(sparql_json(args.query, abox_paths))
    else:
        parser.error(f"unknown command: {args.command}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
