'''Basic tests about the Job class'''

import sys
from six import PY3
import mock

from common import TestQless
from qless.jobs import SubprocessJob


class TestSubprocessJob(TestQless):
    '''Test the SubprocesJob.'''

    def test_basic(self):
        '''Can dispatch the command.'''
        job = mock.Mock(data={
            'command': 'date'
        }, sandbox='/tmp')
        SubprocessJob.process(job)
        self.assertTrue(job.complete.called)

    def test_fails(self):
        '''Fails when we exit with a non-zero status code.'''
        job = mock.Mock(data={
            'command': 'bash',
            'args': ['-c', 'exit 1']
        }, sandbox='/tmp')
        SubprocessJob.process(job)
        self.assertTrue(job.fail.called)

    def test_failure_message(self):
        '''When it fails, take stderr and stdout.'''
        stderr = 'This is stderr'
        stdout = 'This is stdout'
        job = mock.Mock(data={
            'command': 'bash',
            'args': ['-c', '(>&2 echo %s); echo %s; exit 1' % (stderr, stdout)]
        }, sandbox='/tmp')
        SubprocessJob.process(job)
        group, message = job.fail.call_args[0]
        self.assertIn(stderr, message)
        self.assertIn(stdout, message)
