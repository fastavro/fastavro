import fastavro
import json

def main(argv=None):
    import sys
    from argparse import ArgumentParser

    argv = argv or sys.argv

    parser = ArgumentParser(version=fastavro.__version__,
        description='iter over avro file, emit records as JSON')
    parser.add_argument('filename', help='file to parse', nargs='?')
    args = parser.parse_args(argv[1:])

    if args.filename:
        try:
            fo = open(args.filename, 'rb')
        except IOError as e:
            raise SystemExit(
                'error: cannot open {0} - {1}'.format(args.filename, e))
    else:
        fo = sys.stdin

    try:
        for record in fastavro.iter_avro(fo):
            json.dump(record, sys.stdout)
            sys.stdout.write('\n')
    except (IOError, KeyboardInterrupt):
        pass

if __name__ == '__main__':
    main()

