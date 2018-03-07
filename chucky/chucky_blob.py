import hashlib


class Blob:
    """ A blob of data with a hash """
    def __init__(self, data, h):
        self.data = data
        self.h = h

    def __len__(self):
        return len(self.data)


def hash_blob(blob):
    """ Calculate the hash for the given binary blob """
    return hashlib.sha256(blob).digest()
