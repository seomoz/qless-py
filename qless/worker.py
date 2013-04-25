#! /usr/bin/env python

import os
import time
import qless
import shutil
import psutil
import signal
from qless import logger

# Setting the process title
try:
    from setproctitle import setproctitle
except ImportError:
    def setproctitle(title):
        pass


class Worker(object):
    '''Worker. For doing work'''
    # This meta-information about the worker you're running on
    meta = {
        'worker_id': 0,
        'sandbox': '/dev/null'
    }

    def __init__(self, queues, host=None, workers=None, interval=60,
        workdir='.', resume=False):
        host = host or 'localhost'
        _host, _, _port = host.partition(':')
        _port = int(_port or 6379)
        self.host = _host
        self.port = _port
        self.count = workers or psutil.NUM_CPUS
        self.client = qless.client(self.host, self.port)
        self.queues = queues
        self.resume = resume
        self.interval = interval
        self.sandboxes = {}
        # This is for filesystem sandboxing. Each worker has
        # a directory associated with it, which it should make
        # sure is the working directory in which it runs each
        # of the jobs. It should also ensure that the directory
        # exists, and clobbers files before each run, and after.
        self.workdir = os.path.abspath(workdir)
        self.sandbox = self.workdir
        # I'm the parent, so I have a negative worker id
        self.worker_id = -1
        self.master = True
        # These are the job ids that I should get to first, before
        # picking off other jobs
        self.jids = []

    def title(self, message):
        '''Set the title of the process'''
        base = 'qless-py-worker [%s] ' % ','.join(q.name for q in self.queues)
        setproctitle(base + message)
        logger.info(message)

    def run(self):
        '''Run this worker'''
        # If this worker is meant to be resumable, then we should find out
        # what jobs this worker was working on beforehand.
        if self.resume:
            to_resume = self.client.workers[self.client.worker_name]['jobs']
        else:
            to_resume = []

        for i in range(self.count):
            slot = {
                'worker_id': i,
                'sandbox': os.path.join(
                    self.workdir, 'qless-py-workers', 'sandbox-%i' % i)
            }
            cpid = os.fork()
            if cpid:
                logger.info('Spawned worker %i' % cpid)
                self.sandboxes[cpid] = slot
            else:
                # Set the value of the metadata so that jobs can detect
                # what worker they're running on
                Worker.meta = slot
                # Make note that we're not the master, and then save our
                # sandbox and worker id for reference
                self.master = False
                self.sandbox = slot['sandbox']
                self.worker_id = slot['worker_id']
                # Also, we should take our share of the jobs that we want
                # to resume, if any.
                start = (i     * len(to_resume)) / self.count
                end   = ((i+1) * len(to_resume)) / self.count
                self.jids      = to_resume[start:end]
                return self.work()

        while self.master:
            try:
                pid, status = os.wait()
                logger.warn('Worker %i died with status %i from signal %i' % (pid, status >> 8, status & 0xff))
                slot = self.sandboxes.pop(pid)
                cpid = os.fork()
                if cpid:
                    logger.info('Spawned replacement worker %i' % cpid)
                    self.sandboxes[cpid] = slot
                else:
                    # Set the value of the metadata so that jobs can detect
                    # what worker they're running on
                    Worker.meta = slot
                    # Make note that we're not the master, and then save our
                    # sandbox and worker id for reference
                    self.master = False
                    self.sandbox = slot['sandbox']
                    self.worker_id = slot['worker_id']
                    # NOTE: In the case that the worker died, we're going to
                    # assume that something about the job(s) it was working
                    # made the worker exit, and so we're going to ignore any
                    # jobs that we might have been working on. It's also
                    # significantly more difficult than the above problem of
                    # simply distributing work to /new/ workers, rather than
                    # a respawned worker.
                    return self.work()
            except KeyboardInterrupt:
                break
        if self.master:
            self.stop()

    def clean(self):
        '''Clean up the sandbox of all files'''
        os.chdir(self.sandbox)
        # And that it's clear of any files
        for path in os.listdir(self.sandbox):
            path = os.path.join(self.sandbox, path)
            if os.path.isdir(path):
                logger.info('Removing tree %s...' % path)
                shutil.rmtree(path)
            else:
                logger.info('Removing file %s...' % path)
                os.remove(path)

    def work(self):
        '''Keep poppping off jobs and processing them'''
        # We should probably open up our own redis client
        self.client = qless.client(self.host, self.port)
        self.queues = [self.client.queues[q] for q in self.queues]

        if not os.path.isdir(self.sandbox):
            os.makedirs(self.sandbox)
        self.clean()
        # First things first, we should clear out any jobs that
        # we're responsible for off-hand
        while len(self.jids):
            try:
                job = self.client.jobs[self.jids.pop(0)]
                # If we still have access to it, then we should process it
                if job.heartbeat():
                    logger.info('Resuming %s' % job.jid)
                    self.title('Working %s (%s)' % (job.jid, job.klass_name))
                    job.process()
                    self.clean()
                else:
                    logger.warn(
                        'Lost heart on would-be resumed job %s' % job.jid)
            except KeyboardInterrupt:
                return

        while True:
            try:
                seen = False
                for queue in self.queues:
                    job = queue.pop()
                    if job:
                        seen = True
                        self.title('Working %s (%s)' % (job.jid, job.klass_name))
                        job.process()
                        self.clean()
                if not seen:
                    self.title('sleeping...')
                    logger.debug('Sleeping for %fs' % self.interval)
                    time.sleep(self.interval)
            except KeyboardInterrupt:
                return

    def stop(self, sig=signal.SIGINT):
        '''Stop all the workers, and then wait for them'''
        for cpid in self.sandboxes.keys():
            logger.warn('Stopping %i...' % cpid)
            os.kill(cpid, sig)

        while True:
            try:
                pid, _ = os.wait()
                self.sandboxes.pop(pid, None)
                logger.warn('Worker %i stopped.' % pid)
            except OSError:
                break

        for cpid in self.sandboxes.keys():
            logger.warn('Could not wait for %i' % cpid)

    # QUIT - Wait for child to finish processing then exit
    # TERM / INT - Immediately kill child then exit
    # USR1 - Immediately kill child but don't exit
    # USR2 - Don't start to process any new jobs
    # CONT - Start to process new jobs again after a USR2
