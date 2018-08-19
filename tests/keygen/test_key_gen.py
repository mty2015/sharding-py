import unittest

from shardingpy.keygen.base import DefaultKeyGenerator


class DefaultKeyGeneratorTest(unittest.TestCase):

    def test_generate_key(self):
        generator = DefaultKeyGenerator()
        key = generator.generate_key()
        print('=======gen key', key)
