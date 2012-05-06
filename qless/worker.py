#! /usr/bin/env python

import os
import math
import time
import qless
import shutil
import psutil
import signal
import logging
from qless import logger

# This meta-information about the worker you're running on
meta = {
    'worker_id': 0,
    'sandbox'  : '/dev/null'
}

try:
    from setproctitle import setproctitle
except ImportError:
    def setproctitle(title):
        pass

class Worker(object):
    def __init__(self, queues, host='localhost', workers=None, interval=60, workdir='.', resume=False, **kwargs):
        _host, s, _port = host.partition(':')
        _port           = int(_port or 6379)
        self.host       = _host
        self.port       = _port
        self.client     = qless.client(self.host, self.port)
        self.count      = workers or psutil.NUM_CPUS
        self.interval   = interval
        self.queues     = queues
        self.resume     = resume
        self.sandboxes  = {}
        # This is for filesystem sandboxing. Each worker has
        # a directory associated with it, which it should make
        # sure is the working directory in which it runs each
        # of the jobs. It should also ensure that the directory
        # exists, and clobbers files before each run, and after.
        self.workdir    = os.path.abspath(workdir)
        self.sandbox    = self.workdir
        # I'm the parent, so I have a negative worker id
        self.worker_id  = -1
        self.master     = True
        # These are the job ids that I should get to first, before
        # picking off other jobs
        self.jids       = []
    
    def run(self):
        # If this worker is meant to be resumable, then we should find out
        # what jobs this worker was working on beforehand.
        if self.resume:
            jids_to_resume = self.client.workers(self.client.worker)['jobs']
        else:
            jids_to_resume = []
        
        for i in range(self.count):
            slot = {
                'worker_id': i,
                'sandbox'  : os.path.join(self.workdir, 'qless-py-workers', 'sandbox-%i' % i)
            }
            cpid = os.fork()
            if cpid:
                logger.info('Spawned worker %i' % cpid)
                self.sandboxes[cpid] = slot
            else:
                # Set the value of the metadata so that jobs can detect
                # what worker they're running on
                import qless.worker
                qless.worker.meta = slot
                # Make note that we're not the master, and then save our
                # sandbox and worker id for reference
                self.master    = False
                self.sandbox   = slot['sandbox']
                self.worker_id = slot['worker_id']
                # Also, we should take our share of the jobs that we want
                # to resume, if any.
                start = (i     * len(jids_to_resume)) / self.count
                end   = ((i+1) * len(jids_to_resume)) / self.count
                self.jids      = jids_to_resume[start:end]
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
                    import qless.worker
                    qless.worker.meta = slot
                    # Make note that we're not the master, and then save our
                    # sandbox and worker id for reference
                    self.master    = False
                    self.sandbox   = slot['sandbox']
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
        # This cleans the sandbox -- changing the working directory to it,
        # as well as clearing out any files that might be in there
        # Make sure we're running in our sandbox
        os.chdir(self.sandbox)
        # And that it's clear of any files
        for p in os.listdir(self.sandbox):
            p = os.path.join(self.sandbox, p)
            if os.path.isdir(p):
                logger.info('Removing tree %s...' % p)
                shutil.rmtree(p)
            else:
                logger.info('Removing file %s...' % p)
                os.remove(p)
    
    def setproctitle(self, message):
        base = 'qless-py-worker [%s] ' % ','.join(q.name for q in self.queues)
        setproctitle(base + message)
    
    def work(self):
        # We should probably open up our own redis client
        self.client = qless.client(self.host, self.port)
        self.queues = [self.client.queue(q) for q in self.queues]
        
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
                    self.setproctitle('Working %s (%s)' % (job.jid, job.klass_name))
                    job.process()
                    self.clean()
                else:
                    logger.warn('Lost heart on would-be resumed job %s' % job.jid)
            except KeyboardInterrupt:
                return
        
        while True:
            try:
                seen = False
                for queue in self.queues:
                    job = queue.pop()
                    if job:
                        seen = True
                        self.setproctitle('Working %s (%s)' % (job.jid, job.klass_name))
                        job.process()
                        self.clean()
                
                if not seen:
                    self.setproctitle('sleeping...')
                    logger.debug('Sleeping for %fs' % self.interval)
                    time.sleep(self.interval)
            except KeyboardInterrupt:
                return
    
    def stop(self):
        # Stop all the workers, and then wait for them
        for cpid in self.sandboxes.keys():
            logger.warn('Stopping %i...' % cpid)
            os.kill(cpid, signal.SIGINT)
        
        while True:
            try:
                pid, status = os.wait()
                self.sandboxes.pop(pid, None)
                logger.warn('Worker %i stopped.' % cpid)
            except OSError:
                break
        
        for cpid in self.sandboxes.keys():
            logger.warn('Could not wait for %i' % cpid)

    # QUIT - Wait for child to finish processing then exit
    # TERM / INT - Immediately kill child then exit
    # USR1 - Immediately kill child but don't exit
    # USR2 - Don't start to process any new jobs
    # CONT - Start to process new jobs again after a USR2
