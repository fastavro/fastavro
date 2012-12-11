import fastavro as avro
import json
from sys import stdout

encoding = stdout.encoding or "UTF-8"

def main(argv=None):
    import sys
    from argparse import ArgumentParser

    argv = argv or sys.argv

    parser = ArgumentParser(
        description='iter over avro file, emit records as JSON')
    parser.add_argument('file', help='file(s) to parse', nargs='+')
    parser.add_argument('--schema', help='dump schema instead of records',
                        action='store_true', default=False)
    parser.add_argument('--version', action='version',
            version='fastavro {0}'.format(avro.__version__))
    args = parser.parse_args(argv[1:])

    for filename in args.file:
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
            json.dump(reader.schema, stdout, indent=4, encoding=encoding)
            sys.stdout.write('\n')
            continue

        try:
            for record in reader:
                json.dump(record, stdout, encoding=encoding)
                sys.stdout.write('\n')
        except (IOError, KeyboardInterrupt):
            pass

if __name__ == '__main__':
    main()

