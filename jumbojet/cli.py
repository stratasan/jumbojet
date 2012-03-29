import argparse
import os
import json

from core import parse_csv, parse_json, compare_json

def main():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(help="sub-command help")

    parser_csv = subparsers.add_parser('csv')
    parser_csv.add_argument('file')
    parser_csv.set_defaults(func=cmd_csv)

    parser_json = subparsers.add_parser('json')
    parser_json.add_argument('file')
    parser_json.add_argument('class_name', default='SampleClass')
    parser_json.set_defaults(func=cmd_json)

    parser_compare = subparsers.add_parser('compare')
    parser_compare.set_defaults(func=cmd_compare)
    parser_compare.add_argument('file', nargs=2)

    args = parser.parse_args()
    args.func(args, parser)

def cmd_csv(args, parser):
    if not os.path.exists(args.file):
        parser.error("file given does not exist")
    d = parse_csv(args.file)
    print json.dumps(d,indent=4)

def cmd_json(args, parser):
    if not os.path.exists(args.file):
        parser.error("file given does not exist")
    parse_json(args.file, args.class_name)

def cmd_compare(args, parser):
    if not os.path.exists(args.file[0]) or not os.path.exists(args.file[1]):
        parser.error("one of the files given does not exist")
    compare_json(*args.file)

if __name__ == "__main__":
    main()
