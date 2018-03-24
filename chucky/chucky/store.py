from .blob import hash_blob, Blob
from .files import DataStoreFiles


class Chunk:
    """ A blob at an offset """
    def __init__(self, offset, blob):
        self.offset = offset
        self.blob = blob

    def __len__(self):
        return len(self.blob)


class DataStore:
    """ The datastore contains a map to data blobs.

    Each data blob is uniquely identified by its hash.
    """
    def __init__(self):
        self._map = {}
        self._filestore = None

    def __len__(self):
        return len(self._map)

    def save(self, folder):
        """ Save the datastore to disk """
        if not self._filestore:
            self._filestore = DataStoreFiles(folder)

        self._filestore.store(self._map)

    def load(self, folder):
        """ Load datastore from disk"""
        if not self._filestore:
            self._filestore = DataStoreFiles(folder)

        self._filestore.load(self._map)

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
