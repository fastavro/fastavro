import fastavro
import json

def main(argv=None):
    import sys
    from argparse import ArgumentParser

    argv = argv or sys.argv

    parser = ArgumentParser(version=fastavro.__version__,
        description='iter over avro file, emit records as JSON')
    parser.add_argument('filename', help='file to parse', nargs='?')
    parser.add_argument('--schema', help='dump schema and exit',
                        action='store_true', default=False)
    args = parser.parse_args(argv[1:])

    if args.filename:
        try:
            fo = open(args.filename, 'rb')
        except IOError as e:
            raise SystemExit(
                'error: cannot open {0} - {1}'.format(args.filename, e))
    else:
        fo = sys.stdin

    stdout = sys.stdout
    avro = fastavro.iter_avro(fo)
    if args.schema:
        json.dump(avro.schema, stdout, indent=4)
        raise SystemExit

    try:
        for record in avro:
            json.dump(record, stdout)
            sys.stdout.write('\n')
    except (IOError, KeyboardInterrupt):
        pass

if __name__ == '__main__':
    main()

