""" Implementation of the chunking algorithm.

"""


import logging
import hashlib
import statistics

import buzhash


logger = logging.getLogger('chucky')


class Chunk:
    """ A chunk of data """
    def __init__(self, offset, data, h=None):
        self.offset = offset
        self.data = data
        if not h:
            h = hash_blob(data)
        self.h = h

    def __len__(self):
        return len(self.data)


def hash_blob(blob):
    """ Calculate the hash for the given binary blob """
    return hashlib.sha256(blob).digest()


def chop(content):
    # Step 1: divide into chunks:
    chunks = chunk_content(content)

    # Step 2: create hashes for chunks:
    sizes = []
    for chunk in chunks:
        sizes.append(len(chunk))
        print(chunk.h.hex(), len(chunk.h), len(chunk))
    logger.debug(
        'Chunk size on average %s (median=%s)',
        statistics.mean(sizes), statistics.median(sizes))


def chunk_content(content):
    """ Divide content into chunks based on content. """
    logger.debug('Dividing content of %s bytes into chunks', len(content))
    chunk = bytearray()
    chunk_offset = 0
    bh = buzhash.BuzHash()
    Q = 8
    mask = (1 << Q) - 1
    for byte_offset, byte in enumerate(content):
        if bh.digest() & mask == 0:
            yield Chunk(chunk_offset, bytes(chunk))
            chunk = bytearray()
            chunk_offset = byte_offset

        # Append byte to current chunk:
        chunk.append(byte)
        bh.feed(byte)

    yield Chunk(chunk_offset, bytes(chunk))


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
    chop(content)
