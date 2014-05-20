import fastavro as avro
from fastavro.six import json_dump
from sys import stdout

encoding = stdout.encoding or "UTF-8"


def main(argv=None):
    import sys
    from argparse import ArgumentParser

    argv = argv or sys.argv

    parser = ArgumentParser(
        description='iter over avro file, emit records as JSON')
    parser.add_argument('file', help='file(s) to parse', nargs='*')
    parser.add_argument('--schema', help='dump schema instead of records',
                        action='store_true', default=False)
    parser.add_argument('--codecs', help='print supported codecs',
                        action='store_true', default=False)
    parser.add_argument('--version', action='version',
                        version='fastavro {0}'.format(avro.__version__))
    parser.add_argument('-p', '--pretty', help='pretty print json',
                        action='store_true', default=False)
    args = parser.parse_args(argv[1:])

    if args.codecs:
        import fastavro
        print('\n'.join(sorted(fastavro._reader.BLOCK_READERS)))
        raise SystemExit

    files = args.file or ['-']
    for filename in files:
        if filename == '-':
            fo = sys.stdin
        else:
            try:
                fo = open(filename, 'rb')
            except IOError as e:
                raise SystemExit(
                    'error: cannot open {0} - {1}'.format(filename, e))

        try:
            reader = avro.reader(fo)
        except ValueError as e:
            raise SystemExit('error: {0}'.format(e))

        if args.schema:
            json_dump(reader.schema, True)
            sys.stdout.write('\n')
            continue

        indent = 4 if args.pretty else None
        try:
            for record in reader:
                json_dump(record, indent)
                sys.stdout.write('\n')
        except (IOError, KeyboardInterrupt):
            pass

if __name__ == '__main__':
    main()
