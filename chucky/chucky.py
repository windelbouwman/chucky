""" Implementation of the chunking algorithm.

"""


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
        return len(self.blob.data)


class Blob:
    """ A blob of data with a hash """
    def __init__(self, data, h):
        self.data = data
        self.h = h


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


class DataStore:
    """ The datastore contains a map to data blobs.

    Each data blob is uniquely identified by its hash.
    """
    def __init__(self):
        self._map = {}

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
        print(chunk.blob.h.hex(), len(chunk.blob.h), len(chunk))
    stats = ', '.join(
        '{}={}'.format(n, getattr(statistics, n)(sizes))
        for n in ['mean', 'variance', 'stdev'])
    logger.debug('Chunk size stats: %s', stats)
    return ChoppedData(chunks)


def chunk_content(content, data_store):
    """ Divide content into chunks based on content. """
    logger.debug('Dividing content of %s bytes into chunks', len(content))
    chunk = bytearray()
    chunk_offset = 0
    bh = buzhash.BuzHash()
    Q = 8
    mask = (1 << Q) - 1
    for byte_offset, byte in enumerate(content):
        if bh.digest() & mask == 0:
            yield data_store.new_chunk(chunk_offset, bytes(chunk))
            chunk = bytearray()
            chunk_offset = byte_offset

        # Append byte to current chunk:
        chunk.append(byte)
        bh.feed(byte)

    yield data_store.new_chunk(chunk_offset, bytes(chunk))


def serve():
    """ Open a server which can serve the firmware update.

    Endpoints:
    /api/v1/fw/version  -> a file with offsets and hashes
    /api/v1/chunk/<hash>

    """
    raise NotImplementedError('TODO')


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    # Step 1: read content
    with open(__file__, 'r') as f:
        content = f.read().encode('ascii')
    content = content * 10  # Duplicate content on purpose!
    ds = DataStore()
    chop(content, ds)
