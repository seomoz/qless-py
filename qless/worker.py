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

class Worker(Process):
    # This is a class that processes jobs from various queues
    def __init__(self, queues, interval, sandbox):
        Process.__init__(self, target=self.loop)
        self.queues   = queues
        self.interval = interval
        # Our sandbox. Make sure that the directory exists
        self.sandbox  = sandbox
        if not os.path.isdir(self.sandbox):
            os.makedirs(self.sandbox)
    
    def healthy(self):
        # Here we might look for RAM consumption, etc.
        return self.is_alive()
    
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
    
    def loop(self):
        while True:
            try:
                for queue in self.queues:
                    seen = False
                    job = queue.pop()
                    if job:
                        seen = True
                        self.clean()
                        job.process()
                        self.clean()
                    if not seen:
                        time.sleep(self.interval)
            except KeyboardInterrupt:
                break

class Master(object):
    # This class is responsible for managing several children workers
    def __init__(self, queues, host='localhost', port=6579, workers=None, interval=60, workdir='.'):
        self.client    = qless.client(host, port)
        self.count     = workers or psutil.NUM_CPUS
        self.interval  = interval
        self.queues    = [self.client.queue(q) for q in queues]
        self.workers   = []
        # This is for filesystem sandboxing. Each worker has
        # a directory associated with it, which it should make
        # sure is the working directory in which it runs each
        # of the jobs. It should also ensure that the directory
        # exists, and clobbers files before each run, and after.
        self.workdir   = os.path.abspath(workdir)
        self.sandboxes = [os.path.join(self.workdir, 'qless-py-workers', 'sandbox-%i' % i) for i in range(self.count)]
    
    def stop(self, workers=None):
        # Stop all the workers, and then wait for them
        if workers == None:
            workers = self.workers
        for worker in workers:
            logger.warn('Stopping %i' % worker.pid)
            worker.terminate()
        
        for worker in workers:
            worker.join()
            logger.warn('Worker %i stopped' % worker.pid)
            # And return its sandbox to the pool of sandboxes
            self.sandboxes.append(worker.sandbox)
    
    def spawn(self, count):
        workers = [Worker(self.queues, self.interval, self.sandboxes.pop(0)) for i in range(count)]
        self.workers.extend(workers)
        for worker in workers:
            worker.start()
            logger.info('Starting worker %i' % worker.pid)
    
    def run(self):
        self.spawn(self.count)
        while True:
            try:
                # Sleep for a little while, and then check up on the health of all
                # the worker processes.
                time.sleep(60)
                logger.info('Checking workers')
                healthy   = []
                unhealthy = []
                for worker in self.workers:
                    if not worker.healthy():
                        unhealthy.append(worker)
                    else:
                        healthy.append(worker)
                
                logger.info('%i / %i workers unhealthy:' % (len(unhealthy), len(self.workers)))
                for worker in unhealthy:
                    logger.warn('\tWorker %i is unhealthy' % worker.pid)
                # Now kill all the unhealthy ones and then replace them
                self.stop(unhealthy)
                self.workers = healthy
                self.spawn(self.count - len(self.workers))
            except KeyboardInterrupt:
                break
        self.stop()
