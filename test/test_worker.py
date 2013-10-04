'''Test worker'''

# Internal imports
from common import TestQless

import qless
from qless.workers import Worker


class TestWorker(TestQless):
    '''Test the worker'''
    def setUp(self):
        TestQless.setUp(self)
        self.worker = Worker(['foo'], self.client)

    def test_proctitle(self):
        '''Make sure we can get / set the process title'''
        before = Worker.title()
        Worker.title('Foo')
        self.assertNotEqual(before, Worker.title())

    def test_kill(self):
        '''The base worker class' kill method should raise an exception'''
        self.assertRaises(NotImplementedError, self.worker.kill, 1)

    def test_resume(self):
        '''We should be able to resume jobs'''
        queue = self.worker.client.queues['foo']
        queue.put('foo', {})
        job = self.worker.jobs().next()
        self.assertTrue(isinstance(job, qless.Job))
        # Now, we'll create a new worker and make sure it gets that job first
        worker = Worker(['foo'], self.client, resume=[job.jid])
        self.assertEqual(worker.jobs().next().jid, job.jid)

    def test_unresumable(self):
        '''If we can't heartbeat jobs, we should not try to resume it'''
        queue = self.worker.client.queues['foo']
        queue.put('foo', {})
        # Pop from another worker
        other = qless.Client(hostname='other')
        job = other.queues['foo'].pop()
        self.assertTrue(isinstance(job, qless.Job))
        # Now, we'll create a new worker and make sure it gets that job first
        worker = Worker(['foo'], self.client, resume=[job.jid])
        self.assertEqual(worker.jobs().next(), None)
