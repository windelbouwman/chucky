import unittest
import functools
import operator

import chucky


class ChuckyTestCase(unittest.TestCase):
    def setUp(self):
        with open(__file__, 'r') as f:
            content = f.read().encode('ascii')
        self.content = content * 10  # Duplicate content on purpose!

    def test_chopping(self):
        """ Check that restoring after chopping restores original """
        data_store = chucky.DataStore()
        chunks = list(chucky.chunk_content(self.content, data_store))
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
        chopped2 = chucky.ChoppedData.from_recipe(recipe, data_store)
        content2 = chopped2.all_data()
        self.assertEqual(self.content, content2)
