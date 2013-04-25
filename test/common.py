'''A base class for all of our common tests'''

import redis
import unittest

# Qless stuff
import qless
import logging


class TestQless(unittest.TestCase):
    '''Base class for all of our tests'''
    @classmethod
    def setUpClass(cls):
        qless.logger.setLevel(logging.CRITICAL)
        cls.redis = redis.Redis()
        # Clear the script cache, and nuke everything
        cls.redis.execute_command('script', 'flush')

    def setUp(self):
        assert(len(self.redis.keys('*')) == 0)
        # The qless client we're using
        self.client = qless.client()
        self.worker = qless.client()
        self.worker.worker_name = 'worker'

    def tearDown(self):
        # Ensure that we leave no keys behind, and that we've unfrozen time
        self.redis.flushdb()
