'''Test worker'''

# Internal imports
from common import TestQless

import qless
from qless.workers import Worker

# External dependencies
import os


class TestWorker(TestQless):
    '''Test the worker'''
    def setUp(self):
        self.worker = Worker(['foo'])

    def test_proctitle(self):
        '''Make sure we can get / set the process title'''
        before = Worker.title()
        Worker.title('Foo')
        self.assertNotEqual(before, Worker.title())

    def test_clean(self):
        '''Make sure our directory cleaning works fine'''
        directory = 'test/tmp'
        if not os.path.exists(directory):
            os.mkdir(directory)
        self.assertFalse(os.path.exists(os.path.join(directory, 'test_clean')))
        os.mkdir(os.path.join(directory, 'test_clean'))
        with open(os.path.join(directory, 'test_clean_file'), 'w+') as fout:
            fout.write('testing')
        Worker.clean(directory)
        self.assertEqual(os.listdir(directory), [])

    def test_kill(self):
        '''The base worker class' kill method should raise an exception'''
        self.assertRaises(NotImplementedError, self.worker.kill, 1)

    def test_resume(self):
        '''We should be able to resume jobs'''
        self.worker.reconnect()
        queue = self.worker.client.queues['foo']
        queue.put('foo', {})
        job = self.worker.jobs().next()
        self.assertTrue(isinstance(job, qless.Job))
        # Now, we'll create a new worker and make sure it gets that job first
        worker = Worker(['foo'], resume=[job.jid])
        worker.reconnect()
        self.assertEqual(worker.jobs().next().jid, job.jid)

    def test_unresumable(self):
        '''If we can't heartbeat jobs, we should not try to resume it'''
        self.worker.reconnect()
        queue = self.worker.client.queues['foo']
        queue.put('foo', {})
        # Pop from another worker
        other = qless.client(hostname='other')
        job = other.queues['foo'].pop()
        self.assertTrue(isinstance(job, qless.Job))
        # Now, we'll create a new worker and make sure it gets that job first
        worker = Worker(['foo'], resume=[job.jid])
        worker.reconnect()
        self.assertEqual(worker.jobs().next(), None)

if __name__ == '__main__':
    import unittest
    unittest.main()
