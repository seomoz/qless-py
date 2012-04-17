#! /usr/bin/env python

import os
import time
import qless
import shutil
import psutil
import signal
import logging
from qless import logger
from multiprocessing import Process

try:
    from setproctitle import setproctitle
except ImportError:
    def setproctitle(title):
        pass

class Worker(object):
    def __init__(self, queues, host='localhost', port=6579, workers=None, interval=60, workdir='.'):
        self.client    = qless.client(host, port)
        self.count     = workers or psutil.NUM_CPUS
        self.interval  = interval
        self.queues    = [self.client.queue(q) for q in queues]
        self.sandboxes = {}
        # This is for filesystem sandboxing. Each worker has
        # a directory associated with it, which it should make
        # sure is the working directory in which it runs each
        # of the jobs. It should also ensure that the directory
        # exists, and clobbers files before each run, and after.
        self.workdir   = os.path.abspath(workdir)
        self.sandbox   = self.workdir
        self.master    = True
    
    def run(self):
        for i in range(self.count):
            sandbox = os.path.join(self.workdir, 'qless-py-workers', 'sandbox-%i' % i)
            cpid = os.fork()
            if cpid:
                logger.info('Spawned worker %i' % cpid)
                self.sandboxes[cpid] = sandbox
            else:
                self.master  = False
                self.sandbox = sandbox
                self.work()
                return
        
        while self.master:
            try:
                pid, status = os.wait()
                logger.warn('Worker %i died with status %i from signal %i' % (pid, status >> 8, status & 0xff))
                sandbox = self.sandboxes.pop(pid)
                cpid = os.fork()
                if cpid:
                    logger.info('Spawned replacement worker %i' % cpid)
                    self.sandboxes[cpid] = sandbox
                else:
                    self.master  = False
                    self.sandbox = sandbox
                    self.work()
                    return
            except KeyboardInterrupt:
                self.stop(); break
    
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
        if not os.path.isdir(self.sandbox):
            os.makedirs(self.sandbox)
        self.clean()
        while True:
            try:
                seen = False
                for queue in self.queues:
                    job = queue.pop()
                    if job:
                        seen = True
                        logger.info('Processing %s in %s' % (job.jid, queue.name))
                        self.setproctitle('Working %s (%s)' % (job.jid, job.klass))
                        job.process()
                        self.clean()
                
                if not seen:
                    self.setproctitle('sleeping...')
                    time.sleep(self.interval)
            except KeyboardInterrupt:
                break
    
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
