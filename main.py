#!/usr/bin/env python

import fastavro as avro
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
        reader = avro.reader(open(args.filename, 'rb'))
        for r in reader:
            if not args.quiet:
                json.dump(r, sys.stdout)
                sys.stdout.write('\n')
    except IOError:
        pass


if __name__ == '__main__':
    main()

