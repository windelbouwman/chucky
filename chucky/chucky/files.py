import os
import binascii

from .blob import Blob


class DataStoreFiles:
    def __init__(self, location):
        self._location = location
        self._files = []

    def __len__(self):
        return len(self._files)

    def store(self, ds):
        path = os.path.join(self._location, '.chuckstore')

        if not os.path.isdir(path):
            os.mkdir(path)

        for blob in ds:
            hash_group = ds[blob].h[0:2]
            group_path = path + '\\' + binascii.hexlify(hash_group).decode('utf-8')

            if not os.path.isdir(group_path):
                os.mkdir(group_path)

            filename = group_path + '\\' + binascii.hexlify(ds[blob].h).decode('utf-8')

            if not os.path.isfile(filename):
                with open(filename, 'wb') as f:
                    f.write(ds[blob].data)

    def load(self, ds):
        path = os.path.join(self._location, '.chuckstore')

        if not os.path.isdir(path):
            return

        for root, dirs, files in os.walk(path, topdown=False):
            for name in files:
                with open(os.path.join(root, name), 'rb') as f:
                    data = f.read()
                    hash = binascii.unhexlify(name)

                    blob = Blob(data, hash)
                    ds[hash] = blob
