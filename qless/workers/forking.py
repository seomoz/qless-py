'''A worker that forks child processes'''

import os
import psutil
import signal
import itertools
from qless import logger

from . import Worker
from .serial import SerialWorker


class ForkingWorker(Worker):
    '''A worker that forks child processes'''
    def __init__(self, *args, **kwargs):
        Worker.__init__(self, *args, **kwargs)
        # Worker class to use
        self.klass = self.kwargs.pop('klass', SerialWorker)
        # How many children to launch
        self.count = self.kwargs.pop('workers', psutil.NUM_CPUS)
        # A dictionary of child pids to information about them
        self.sandboxes = {}
        # Whether or not we're supposed to shutdown
        self.shutdown = False

    def stop(self, sig=signal.SIGINT):
        '''Stop all the workers, and then wait for them'''
        for cpid in self.sandboxes.keys():
            logger.warn('Stopping %i...' % cpid)
            os.kill(cpid, sig)

        # While we still have children running, wait for them
        for cpid in self.sandboxes.keys():
            try:
                logger.info('Waiting for %i...' % cpid)
                pid, status = os.waitpid(cpid, 0)
                logger.warn('%i stopped with status %i' % (pid, status >> 8))
            except OSError:  # pragma: no cover
                logger.exception('Error waiting for %i...' % cpid)
            finally:
                self.sandboxes.pop(pid, None)

    def spawn(self, **kwargs):
        '''Return a new worker for a child process'''
        copy = dict(self.kwargs)
        copy.update(kwargs)
        return self.klass(self.queues, self.client, **copy)

    def run(self):
        '''Run this worker'''
        self.signals(('TERM', 'INT', 'QUIT'))
        # Divide up the jobs that we have to divy up between the workers. This
        # produces evenly-sized groups of jobs
        resume = self.divide(self.resume, self.count)
        for index in range(self.count):
            cpid = os.fork()
            if cpid:
                logger.info('Spawned worker %i' % cpid)
                self.sandboxes[cpid] = {}
            else:  # pragma: no cover
                self.spawn(resume=resume[index]).run()
                exit(0)

        try:
            while not self.shutdown:
                pid, status = os.wait()
                logger.warn('Worker %i died with status %i from signal %i' % (
                    pid, status >> 8, status & 0xff))
                slot = self.sandboxes.pop(pid)
                cpid = os.fork()
                if cpid:
                    logger.info('Spawned replacement worker %i' % cpid)
                    self.sandboxes[cpid] = slot
                else:  # pragma: no cover
                    self.spawn().run()
                    exit(0)
        finally:
            self.stop(signal.SIGKILL)

    def handler(self, signum, frame):  # pragma: no cover
        '''Signal handler for this process'''
        if signum in (signal.SIGTERM, signal.SIGINT, signal.SIGQUIT):
            for cpid in self.sandboxes.keys():
                os.kill(cpid, signum)
            exit(0)
