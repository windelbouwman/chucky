""" Implementation of the chunking algorithm.

"""


import argparse
import logging
import glob

from .store import DataStore
from .chopper import chop


logger = logging.getLogger('chucky')


def serve():
    """ Open a server which can serve the firmware update.

    Endpoints:
    /api/v1/fw/version  -> a file with offsets and hashes
    /api/v1/chunk/<hash>

    """
    raise NotImplementedError('TODO')


def compare(filenames):
    """ Compares files to check for equal chunks """
    logging.info('Comparing %s', filenames)
    # Create table:
    rows = []
    for filename1 in filenames:
        row = []
        for filename2 in filenames:
            r = compare_two_files(filename1, filename2)
            row.append(r)
        rows.append(row)
    print(rows)


def compare_two_files(filename1, filename2):
    ds = DataStore()
    logging.info('Comparing %s and %s', filename1, filename2)
    with open(filename1, 'rb') as file1:
        chopped1 = chop(file1.read(), ds)
    with open(filename2, 'rb') as file2:
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
    got_percent = have_size * 100 / chopped2.size
    return got_percent


def visualize():
    # TODO generate html file with graphical view
    pass


def main():
    logging.basicConfig(level=logging.DEBUG)
    parser = argparse.ArgumentParser()
    sp = parser.add_subparsers(dest='command')
    compare_parser = sp.add_parser('compare')
    compare_parser.add_argument('files', nargs='+')
    args = parser.parse_args()

    if args.command == 'compare':
        filenames = []
        for pattern in args.files:
            filenames.extend(glob.iglob(pattern))
        compare(filenames)
    elif args.command == 'chop':
        # Step 1: read content
        with open(__file__, 'r') as f:
            content = f.read().encode('ascii')
        content = content * 10  # Duplicate content on purpose!
        ds = DataStore()
        chop(content, ds)
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
