'''Test the forking worker'''

# Internal imports
from common import TestQless

import os
import time
import signal
import threading

# The stuff we're actually testing
import qless
from qless.workers import Worker
from qless.workers.forking import ForkingWorker


class Foo(object):
    '''Dummy class'''
    @staticmethod
    def foo(job):
        '''Fall on your sword!'''
        os.kill(os.getpid(), signal.SIGKILL)


class TestWorker(TestQless):
    '''Test the worker'''
    def setUp(self):
        TestQless.setUp(self)
        self.worker = ForkingWorker(['foo'], self.client, workers=1, interval=1)
        self.queue = self.client.queues['foo']
        self.thread = None

    def tearDown(self):
        if self.thread:
            self.thread.join()
        TestQless.tearDown(self)

    def test_respawn(self):
        '''It respawns workers as needed'''
        self.thread = threading.Thread(target=self.worker.run)
        self.thread.start()
        time.sleep(0.1)
        self.worker.shutdown = True
        self.queue.put(Foo, {})
        self.thread.join(1)
        self.assertFalse(self.thread.is_alive())

    def test_spawn(self):
        '''It gives us back a worker instance'''
        self.assertTrue(isinstance(self.worker.spawn(), Worker))
