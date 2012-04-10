#! /usr/bin/env python

import os
import time
import qless
import psutil
import signal
import logging
from multiprocessing import Process

logger = logging.getLogger('qless')
formatter = logging.Formatter('%(asctime)s | PID %(process)d | [%(levelname)s] %(message)s')
handler = logging.StreamHandler()
handler.setLevel(logging.DEBUG)
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)

class Worker(Process):
    # This is a class that processes jobs from various queues
    def __init__(self, client, interval, queues):
        self.client   = client
        self.interval = interval
        self.queues   = [client.queue(q) for q in queues]
        Process.__init__(self, target=self.loop)
    
    def healthy(self):
        # Here we might look for RAM consumption, etc.
        return self.is_alive()
    
    def loop(self):
        while True:
            try:
                for queue in self.queues:
                    seen = False
                    job = queue.pop()
                    if job:
                        seen = True
                        job.process()
                
                    if not seen:
                        time.sleep(self.interval)
            except KeyboardInterrupt:
                break

class Master(object):
    # This class is responsible for managing several children workers
    def __init__(self, host, port, workers, interval, queues):
        self.client   = qless.client(host, port)
        self.count    = workers
        self.interval = interval
        self.queues   = queues
        self.workers  = []
    
    def pop(self):
        for i in range(len(self._queues)):
            q = iter(self.queues).next()
            job = q.pop()
            if job:
                return job
    
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
    
    def spawn(self, count):
        workers = [Worker(self.client, self.interval, self.queues) for i in range(count)]
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
