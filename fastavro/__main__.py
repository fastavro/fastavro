import json
import datetime
from decimal import Decimal
from sys import stdout
from uuid import UUID
from platform import python_version_tuple

import fastavro as avro
from fastavro.six import json_dump, btou

encoding = stdout.encoding or "UTF-8"


class CleanJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime.date, datetime.datetime)):
            return obj.isoformat()
        elif isinstance(obj, (Decimal, UUID)):
            return str(obj)
        elif isinstance(obj, bytes):
            return btou(obj, encoding='iso-8859-1')
        else:
            return json.JSONEncoder.default(self, obj)


def main(argv=None):
    import sys
    from argparse import ArgumentParser

    argv = argv or sys.argv

    parser = ArgumentParser(
        description='iter over avro file, emit records as JSON')
    parser.add_argument('file', help="file(s) to parse, use `-' for stdin",
                        nargs='*')
    parser.add_argument('--schema', help='dump schema instead of records',
                        action='store_true', default=False)
    parser.add_argument('--metadata', help='dump metadata instead of records',
                        action='store_true', default=False)
    parser.add_argument('--codecs', help='print supported codecs',
                        action='store_true', default=False)
    parser.add_argument('--version', action='version',
                        version='fastavro %s' % avro.__version__)
    parser.add_argument('-p', '--pretty', help='pretty print json',
                        action='store_true', default=False)
    args = parser.parse_args(argv[1:])

    if args.codecs:
        print('\n'.join(sorted(avro.read.BLOCK_READERS)))
        exit(0)

    files = args.file or ['-']
    for filename in files:
        if filename == '-':
            if python_version_tuple() >= ('3',):
                fo = sys.stdin.buffer
            else:
                fo = sys.stdin
        else:
            fo = open(filename, 'rb')

        reader = avro.reader(fo)

        if args.schema:
            json_dump(reader.schema, indent=4)
            sys.stdout.write('\n')
            continue

        elif args.metadata:
            del reader.metadata['avro.schema']
            json_dump(reader.metadata, indent=4)
            sys.stdout.write('\n')
            continue

        indent = 4 if args.pretty else None
        for record in reader:
            json_dump(record, indent=indent, cls=CleanJSONEncoder)
            sys.stdout.write('\n')
            sys.stdout.flush()


if __name__ == '__main__':
    main()
