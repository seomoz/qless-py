'''Test the serial worker'''

# Internal imports
from common import TestQless

import time

# The stuff we're actually testing
import qless
from qless.workers.greenlet import GeventWorker


class GeventJob(object):
    '''Dummy class'''
    @staticmethod
    def foo(job):
        '''Dummy job'''
        job.complete()


class PatchedGeventWorker(GeventWorker):
    '''A worker that limits the number of jobs it runs'''
    @classmethod
    def patch(cls):
        '''Don't monkey-patch anything'''
        pass

    def jobs(self):
        '''Yield only a few jobs'''
        generator = GeventWorker.jobs(self)
        for _ in xrange(5):
            yield generator.next()

    def listen(self):
        '''Don't actually listen for pubsub events'''
        pass


class TestWorker(TestQless):
    '''Test the worker'''
    def setUp(self):
        TestQless.setUp(self)
        self.client = qless.client()
        self.queue = self.client.queues['foo']
        self.thread = None

    def tearDown(self):
        if self.thread:
            self.thread.join()
        TestQless.tearDown(self)

    def test_basic(self):
        '''Can complete jobs in a basic way'''
        jids = [self.queue.put(GeventJob, {}) for _ in xrange(5)]
        PatchedGeventWorker(['foo'], pool_size=1, interval=0.2).run()
        states = [self.client.jobs[jid].state for jid in jids]
        self.assertEqual(states, ['complete'] * 5)


if __name__ == '__main__':
    import unittest
    unittest.main()
