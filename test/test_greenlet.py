'''Test the serial worker'''

# Internal imports
from common import TestQless

import time
import gevent

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
        self.worker = PatchedGeventWorker(
            ['foo'], self.client, greenlets=1, interval=0.2)
        self.queue = self.client.queues['foo']
        self.thread = None

    def tearDown(self):
        if self.thread:
            self.thread.join()
        TestQless.tearDown(self)

    def test_basic(self):
        '''Can complete jobs in a basic way'''
        jids = [self.queue.put(GeventJob, {}) for _ in xrange(5)]
        self.worker.run()
        states = [self.client.jobs[jid].state for jid in jids]
        self.assertEqual(states, ['complete'] * 5)

    def test_sleeps(self):
        '''Make sure the client sleeps if there aren't jobs to be had'''
        for _ in xrange(4):
            self.queue.put(GeventJob, {})
        before = time.time()
        self.worker.run()
        self.assertGreater(time.time() - before, 0.2)

    def test_kill(self):
        '''Can kill greenlets when it loses its lock'''
        worker = PatchedGeventWorker(['foo'], self.client)
        greenlet = gevent.spawn(gevent.sleep, 100)
        worker.greenlets['foo'] = greenlet
        worker.kill('foo')
        greenlet.join()
        self.assertTrue(isinstance(greenlet.value, gevent.GreenletExit))

    def test_kill_dead(self):
        '''Does not panic if the greenlet handling a job is no longer around'''
        # This test succeeds if it finishes without an exception
        self.worker.kill('foo')
