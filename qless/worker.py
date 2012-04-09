#! /usr/bin/env python

import time
import qless

class Worker(object):
    # This is a class that processes jobs from various queues
    def __init__(self, host, port, interval, queues):
        self.interval = interval
        self.queues   = queues
        self.client   = qless.client(host, port)
        self.queues   = [self.client.queue(q) for q in queues]
    
    def healthy(self):
        return True
    
    def run(self):
        while True:
            try:
                seen = False
                for queue in self.queues:
                    job = queue.pop()
                    if job:
                        seen = True
                        job.process()
                # Sleep for a little while before trying again
                if not seen:
                    print 'Sleeping...'
                    time.sleep(self.interval)
            except KeyboardInterrupt:
                pass

class Master(object):
    # This class is responsible for managing several children workers
    def __init__(self, host, port, workers, interval, queues):
        self.host     = host
        self.port     = port
        self.count    = workers
        self.interval = interval
        self.queues   = queues
        self.workers  = []
    
    def run(self):
        self.workers = [Worker(self.host, self.port, self.interval, self.queues) for i in range(self.count)]
        for worker in self.workers:
            try:
                worker.run()
            except KeyboardInterrupt:
                break
        # At this point, the master should periodically check each of the workers
        # for health, and then preempt that worker