'''Test worker'''

# Internal imports
from common import TestQless

import qless
from qless.workers import Worker

# External dependencies
import os
import itertools


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

    def test_clean(self):
        '''Should be able to clean a directory'''
        self.assertEqual(os.listdir('test/tmp'), [])
        os.makedirs('test/tmp/foo/bar')
        with open('test/tmp/file.out', 'w+'):
            pass
        self.assertNotEqual(os.listdir('test/tmp'), [])
        Worker.clean('test/tmp')
        self.assertEqual(os.listdir('test/tmp'), [])

    def test_sandbox(self):
        '''The sandbox utility should work'''
        path = 'test/tmp/foo'
        self.assertFalse(os.path.exists(path))
        try:
            with Worker.sandbox(path):
                self.assertTrue(os.path.exists(path))
                for name in ['whiz', 'widget', 'bang']:
                    with open(os.path.join(path, name), 'w+'):
                        pass
                # Now raise an exception
                raise ValueError('foo')
        except ValueError:
            pass
        # Make sure the directory has been cleaned
        self.assertEqual(os.listdir(path), [])
        os.rmdir(path)

    def test_sandbox_exists(self):
        '''Sandbox creation should not throw an error if the path exists'''
        path = 'test/tmp'
        self.assertEqual(os.listdir(path), [])
        with Worker.sandbox(path):
            pass
        # If we get to this point, the test succeeds
        self.assertTrue(True)

    def test_dirty_sandbox(self):
        '''If a sandbox is dirty on arrival, clean it first'''
        path = 'test/tmp/foo'
        with Worker.sandbox(path):
            for name in ['whiz', 'widget', 'bang']:
                with open(os.path.join(path, name), 'w+'):
                    pass
            # Now it's sullied. Clean it up
            self.assertNotEqual(os.listdir(path), [])
            with Worker.sandbox(path):
                self.assertEqual(os.listdir(path), [])
        os.rmdir(path)

    def test_resume(self):
        '''We should be able to resume jobs'''
        queue = self.worker.client.queues['foo']
        queue.put('foo', {})
        job = self.worker.jobs().next()
        self.assertTrue(isinstance(job, qless.Job))
        # Now, we'll create a new worker and make sure it gets that job first
        worker = Worker(['foo'], self.client, resume=[job])
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
        worker = Worker(
            ['foo'], self.client, resume=[self.client.jobs[job.jid]])
        self.assertEqual(worker.jobs().next(), None)

    def test_resumable(self):
        '''We should be able to find all the jobs that can be resumed'''
        # We're going to put some jobs into some queues, and pop them.
        jid = self.client.queues['foo'].put('Foo', {})
        self.client.queues['bar'].put('Foo', {})
        self.client.queues['foo'].pop()
        self.client.queues['bar'].pop()

        # Now, we should be able to see a resumable job in 'foo', but we should
        # not see the job that we popped from 'bar'
        worker = Worker(['foo'], self.client, resume=True)
        jids = [job.jid for job in worker.resume]
        self.assertEqual(jids, [jid])

    def test_divide(self):
        '''We should be able to divide resumable jobs evenly'''
        items = self.worker.divide(range(100), 7)
        # Make sure we have the same items as output as input
        self.assertEqual(sorted(itertools.chain(*items)), range(100))
        lengths = [len(batch) for batch in items]
        self.assertLessEqual(max(lengths) - min(lengths), 1)
