import binascii

class DataStoreFiles:
    def __init__(self, location):
        self._location = location
        self._files = []

    def __len__(self):
        return len(self.files)

    def store(self, ds):
        for blob in ds:
            filename = binascii.hexlify(ds[blob].h).decode('utf-8')
            with open(filename, 'wb') as f:
                f.write(ds[blob].data)

