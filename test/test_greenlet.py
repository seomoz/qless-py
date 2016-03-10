'''Test the serial worker'''

# Internal imports
from common import TestQless

import time
import gevent

# The stuff we're actually testing
from qless.workers.greenlet import GeventWorker


class GeventJob(object):
    '''Dummy class'''
    @staticmethod
    def foo(job):
        '''Dummy job'''
        job.data['sandbox'] = job.sandbox
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
        for i, job in enumerate(generator):
            if i >= 5:
                break
            yield job

    def listen(self, _):
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
        jids = [self.queue.put(GeventJob, {}) for _ in range(5)]
        self.worker.run()
        states = [self.client.jobs[jid].state for jid in jids]
        self.assertEqual(states, ['complete'] * 5)
        sandboxes = [self.client.jobs[jid].data['sandbox'] for jid in jids]
        for sandbox in sandboxes:
            self.assertIn('qless-py-workers/greenlet-0', sandbox)

    def test_sleeps(self):
        '''Make sure the client sleeps if there aren't jobs to be had'''
        for _ in range(4):
            self.queue.put(GeventJob, {})
        before = time.time()
        self.worker.run()
        self.assertGreater(time.time() - before, 0.2)

    def test_kill(self):
        '''Can kill greenlets when it loses its lock'''
        worker = PatchedGeventWorker(['foo'], self.client)
        greenlet = gevent.spawn(gevent.sleep, 1)
        worker.greenlets['foo'] = greenlet
        worker.kill('foo')
        greenlet.join()
        self.assertIsInstance(greenlet.value, gevent.GreenletExit)

    def test_kill_dead(self):
        '''Does not panic if the greenlet handling a job is no longer around'''
        # This test succeeds if it finishes without an exception
        self.worker.kill('foo')
