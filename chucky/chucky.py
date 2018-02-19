""" Implementation of the chunking algorithm.

"""


import os
import argparse
import logging

from chucky_store import DataStore
from chucky_chopper import chop


logger = logging.getLogger('chucky')


def serve():
    """ Open a server which can serve the firmware update.

    Endpoints:
    /api/v1/fw/version  -> a file with offsets and hashes
    /api/v1/chunk/<hash>

    """
    raise NotImplementedError('TODO')


def compare(file1, file2):
    """ Compare two files to check for equal chunks """
    ds = DataStore()
    chopped1 = chop(file1.read(), ds)
    chopped2 = chop(file2.read(), ds)

    #ds.save(os.path.dirname(os.path.realpath(__file__)))

    #ds_loaded = DataStore()
    #ds_loaded.load(os.path.dirname(os.path.realpath(__file__)))

    # print(chopped1)
    # print(chopped2)
    h1_set = set(c.blob.h for c in chopped1.chunks)
    # print(h1_set)
    # h2_set = set(c.blob.h for c in chopped2.chunks)
    # print(h1_set.union(h2_set))
    # Upa
    new_size = 0
    have_size = 0
    for chunk in chopped2.chunks:
        if chunk.blob.h in h1_set:
            # print('blob already present', chunk.blob.h)
            have_size += len(chunk)
        else:
            # print('blob is new', chunk.blob.h)
            new_size += len(chunk)
    print(
        '{} new bytes ({} %).'.format(
            new_size, new_size * 100 / chopped2.size))
    print(
        '{} bytes already got ({} %)'.format(
            have_size, have_size * 100 / chopped2.size))

    # TODO generate html file with graphical view


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    parser = argparse.ArgumentParser()
    sp = parser.add_subparsers(dest='command')
    compare_parser = sp.add_parser('compare')
    compare_parser.add_argument('file1', type=argparse.FileType('rb'))
    compare_parser.add_argument('file2', type=argparse.FileType('rb'))
    args = parser.parse_args()

    if args.command == 'compare':
        compare(args.file1, args.file2)
    elif args.command == 'chop':
        # Step 1: read content
        with open(__file__, 'r') as f:
            content = f.read().encode('ascii')
        content = content * 10  # Duplicate content on purpose!
        ds = DataStore()
        chop(content, ds)
    else:
        raise NotImplementedError(args.command)
