'''Test worker'''

# Internal imports
from common import TestQless

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
