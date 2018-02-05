import unittest
import functools
import operator

from chucky import chunk_content


class ChuckyTestCase(unittest.TestCase):
    def test_chopping(self):
        """ Check that restoring after chopping restores original """
        with open(__file__, 'r') as f:
            content = f.read().encode('ascii')
        content = content * 10  # Duplicate content on purpose!
        chunks = list(chunk_content(content))
        self.assertGreater(len(chunks), 1)
        content2 = functools.reduce(operator.add, (c.data for c in chunks))
        self.assertEqual(content, content2)
