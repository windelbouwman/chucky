import unittest
import functools
import operator

import chucky
from chucky.chopper import chunk_content
from chucky.chopper import ChoppedData
from chucky import buzhash


class ChuckyChopperTestCase(unittest.TestCase):
    def setUp(self):
        self.content = bytes(range(10, 100)) * 100

    def test_chopping(self):
        """ Check that restoring after chopping restores original """
        data_store = chucky.DataStore()
        chunks = list(chunk_content(self.content, data_store, Q=4))
        self.assertGreater(len(chunks), 1)
        content2 = functools.reduce(
            operator.add,
            (c.blob.data for c in chunks))
        self.assertEqual(self.content, content2)

    def test_reconstruction(self):
        """ Chop a piece of data and restore from list of hashes """
        data_store = chucky.DataStore()
        chopped = chucky.chop(self.content, data_store)
        self.assertEqual(self.content, chopped.all_data())
        recipe = chopped.serialize()
        chopped2 = ChoppedData.from_recipe(recipe, data_store)
        content2 = chopped2.all_data()
        self.assertEqual(self.content, content2)


class BuzHashTestCase(unittest.TestCase):
    def test_rotate_left(self):
        value = 0x12345678
        v2 = buzhash.bsl(value, 4)
        self.assertEqual(0x23456781, v2)
        v3 = buzhash.bsl(value, 32)
        self.assertEqual(0x12345678, v3)
        v4 = buzhash.bsl(value, 0)
        self.assertEqual(0x12345678, v4)
        v5 = buzhash.bsl(value, 16)
        self.assertEqual(0x56781234, v5)

    def test_sliding_hash(self):
        """ Test that hash sliding works """
        data = bytes(range(10, 100))

        # Hash window 1:
        window1 = data[0:16]
        hash1 = buzhash.hash_data(data[0:16])
        bh = buzhash.BuzHash()
        for byte in window1:
            bh.feed(byte)
        self.assertEqual(hash1, bh.digest())

        # Proceed to sliding window 2:
        bh.slide(data[16], data[0], window_size=16)
        window2 = data[1:17]
        hash2 = buzhash.hash_data(window2)
        self.assertEqual(bh.digest(), hash2)

    def test_split_data(self):
        """ Test that hash sliding works """
        data = bytes(range(10, 100))
        chunks = list(buzhash.split_data(data, Q=4))
        self.assertGreater(len(chunks), 1)
        data2 = functools.reduce(
            operator.add,
            (c[1] for c in chunks))
        self.assertEqual(data2, data)

        pos = 0
        for offset, chunk in chunks:
            self.assertEqual(pos, offset)
            self.assertEqual(data[offset], chunk[0])
            pos += len(chunk)
        self.assertEqual(pos, len(data))
