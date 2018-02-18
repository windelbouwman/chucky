""" Implementation of the chunking algorithm.

"""


import argparse
import logging
import hashlib
import statistics
import functools
import operator

import buzhash


logger = logging.getLogger('chucky')


class Chunk:
    """ A blob at an offset """
    def __init__(self, offset, blob):
        self.offset = offset
        self.blob = blob

    def __len__(self):
        return len(self.blob)


class Blob:
    """ A blob of data with a hash """
    def __init__(self, data, h):
        self.data = data
        self.h = h

    def __len__(self):
        return len(self.data)


class ChoppedData:
    """ Data chopped into chunks """
    def __init__(self, chunks):
        self.chunks = chunks

    def serialize(self):
        """ Create a table which describes the chopped data. """
        return [(c.offset, c.blob.h) for c in self.chunks]

    @classmethod
    def from_recipe(cls, recipe, data_store):
        """ Given a a structure of data, restore """
        chunks = [data_store.get_chunk(offset, h) for offset, h in recipe]
        return cls(chunks)

    def all_data(self):
        """ Return whole data of this chopped data """
        return functools.reduce(
            operator.add,
            (c.blob.data for c in self.chunks))

    @property
    def size(self):
        return sum(len(chunk) for chunk in self.chunks)


class DataStore:
    """ The datastore contains a map to data blobs.

    Each data blob is uniquely identified by its hash.
    """
    def __init__(self):
        self._map = {}

    def save(self, folder):
        """ Save the datastore to disk """
        # TODO: Implement
        raise NotImplementedError('TODO')

    def get_blob(self, h):
        """ Given a hash, get the blob """
        return self._map[h]

    def unique_blob(self, data):
        """ Get the unique blob object for the given data.

        If the data is already present, return that blob. Otherwise create
        a new blob.
        """
        h = hash_blob(data)
        if h in self._map:
            blob = self._map[h]
            assert blob.data == data
        else:
            blob = Blob(data, h)
            self._map[h] = blob
        return blob

    def get_chunk(self, offset, h):
        """ Given a position and a hash get a chunk with """
        return Chunk(offset, self.get_blob(h))

    def new_chunk(self, offset, data):
        """ Create a new chunk of data a the given offset """
        blob = self.unique_blob(data)
        return Chunk(offset, blob)


def hash_blob(blob):
    """ Calculate the hash for the given binary blob """
    return hashlib.sha256(blob).digest()


def chop(content, data_store):
    """ Chop data and return chopped data class """
    # Step 1: divide into chunks:
    chunks = list(chunk_content(content, data_store))

    # Step 2: create hashes for chunks:
    sizes = []
    for chunk in chunks:
        sizes.append(len(chunk))
        # print(
        #    'hash:', chunk.blob.h.hex(),
        #    # 'blob hash length:', len(chunk.blob.h),
        #    'blob size:', len(chunk))
    if len(chunks) > 1:
        logger.debug('%s chunks', len(chunks))
        stats = ', '.join(
            '{}={}'.format(n, getattr(statistics, n)(sizes))
            for n in ['mean', 'variance', 'stdev'])
        logger.debug('Chunk size stats: %s', stats)
    else:
        logger.debug('Only one chunk')
    return ChoppedData(chunks)


def chunk_content(content, data_store, Q=9):
    """ Divide content into chunks based on content. """
    logger.debug('Dividing content of %s bytes into chunks', len(content))
    for offset, chunk in buzhash.split_data(content, Q=Q):
        yield data_store.new_chunk(offset, chunk)


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
