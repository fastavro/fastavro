#!/usr/bin/env python

from fastavro import iter_avro
import json

def main(argv=None):
    import sys
    from argparse import ArgumentParser

    argv = argv or sys.argv

    parser = ArgumentParser(description='iter over avro file')
    parser.add_argument('filename', help='file to parse')
    parser.add_argument('-q', '--quiet', help='be quiet', default=False,
                        action='store_true')
    args = parser.parse_args(argv[1:])

    try:
        for r in iter_avro(open(args.filename, 'rb')):
            if not args.quiet:
                json.dump(r, sys.stdout)
                sys.stdout.write('\n')
    except IOError:
        pass


if __name__ == '__main__':
    main()

