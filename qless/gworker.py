#! /usr/bin/env python

import os
import qless
import simplejson as json
from qless import worker, logger


class Worker(worker.Worker):
    '''A Gevent-based worker'''
    def __init__(self, *args, **kwargs):
        self.greenlets = {}
        self.pool_size = kwargs.pop('pool_size', 10)
        worker.Worker.__init__(self, *args, **kwargs)

    def process(self, job):
        '''Process a job'''
        job.process()
        # After processing the job, delete its entry from our greenlets mapping
        if not self.greenlets.pop(job.jid, None):
            logger.warn('Missing greenlet for job %s' % job.jid)

    def listen(self):
        '''Listen for pubsub messages'''
        pubsub = self.client.redis.pubsub()
        pubsub.subscribe([self.client.worker_name])
        for message in pubsub.listen():
            if message['type'] != 'message':
                continue

            # If anything has happened to affect our ownership of a job, we
            # should kill that worker
            data = json.loads(message['data'])
            if data['event'] in ('canceled', 'lock lost', 'put'):
                jid = data['jid']
                logger.warn('Lost ownerhsip of job %s' % jid)
                greenlet = self.greenlets.get(jid)
                if not greenlet:
                    logger.warn('Worker for %s already dead' % jid)
                else:
                    greenlet.kill()

    def work(self):
        # We should probably open up our own redis client
        self.client = qless.client(self.host, self.port)
        self.queues = [self.client.queues[q] for q in self.queues]

        if not os.path.isdir(self.sandbox):
            os.makedirs(self.sandbox)

        from gevent.pool import Pool
        from gevent import sleep, Greenlet
        from gevent import monkey; monkey.patch_all()

        # Start listening
        Greenlet(self.listen).start()

        pool = Pool(self.pool_size)
        while True:
            try:
                seen = False
                for queue in self.queues:
                    # Wait until a greenlet is available
                    pool.wait_available()
                    job = queue.pop()
                    if job:
                        # For whatever reason, doing imports within a greenlet
                        # (there's one implicitly invoked in job.process), was
                        # throwing exceptions. The relatively ghetto way to get
                        # around this is to force the import to happen before
                        # the greenlet is spawned.
                        _module = job.klass
                        seen = True
                        greenlet = Greenlet(self.process, job)
                        self.greenlets[job.jid] = greenlet
                        pool.start(greenlet)

                if not seen:
                    logger.debug('Sleeping for %fs' % self.interval)
                    sleep(self.interval)
            except KeyboardInterrupt:
                return
