from . import buzhash

import logging
import statistics
import functools
import operator

logger = logging.getLogger('chucky')


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
