'''Test worker'''

# Internal imports
from common import TestQless

# External dependencies
import os


class Foo(object):
    '''Dummy class'''
    @staticmethod
    def foo(job):
        '''Dummy job'''
        job.data['cwd'] = os.getcwd()
        job.complete()


class TestWorker(TestQless):
    '''Test the worker'''
    pass
