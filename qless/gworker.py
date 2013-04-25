'''A Gevent-based worker'''

#! /usr/bin/env python

from __future__ import print_function
import os
import redis
import qless
import gevent
import gevent.pool
import simplejson as json
from qless import worker, logger


class Worker(worker.Worker):
    '''A Gevent-based worker'''
    def __init__(self, *args, **kwargs):
        self.greenlets = {}
        self.pool = gevent.pool.Pool(kwargs.pop('pool_size', 10))
        worker.Worker.__init__(self, *args, **kwargs)

    def process(self, job):
        '''Process a job'''
        job.process()
        # After processing the job, delete its entry from our greenlets mapping
        if not self.greenlets.pop(job.jid, None):
            logger.warn('Missing greenlet for job %s' % job.jid)

    def listen(self):
        '''Listen for pubsub messages'''
        logger.info('Listening on pubsub')
        pubsub = redis.Redis(self.host, self.port).pubsub()
        pubsub.subscribe(['ql:w:' + self.client.worker_name])
        for message in pubsub.listen():
            if message['type'] != 'message':
                continue

            # If anything has happened to affect our ownership of a job, we
            # should kill that worker
            data = json.loads(message['data'])
            if data['event'] in ('canceled', 'lock_lost', 'put'):
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
            logger.info('Making sandbox: %s' % self.sandbox)
            os.makedirs(self.sandbox)

        # Start listening
        gevent.spawn(self.listen)

        from gevent import monkey
        monkey.patch_all()

        while True:
            try:
                seen = False
                for queue in self.queues:
                    logger.debug('Checking queue %s' % queue.name)
                    # Wait until a greenlet is available
                    self.pool.wait_available()
                    job = queue.pop()
                    if job:
                        # For whatever reason, doing imports within a greenlet
                        # (there's one implicitly invoked in job.process), was
                        # throwing exceptions. The hacky way to get around this
                        # is to force the import to happen before the greenlet
                        # is spawned.
                        logger.info('Popped job %s' % job.jid)
                        seen = bool(job.klass)
                        greenlet = gevent.Greenlet(self.process, job)
                        self.greenlets[job.jid] = greenlet
                        self.pool.start(greenlet)

                if not seen:
                    logger.debug('Sleeping for %fs' % self.interval)
                    gevent.sleep(self.interval)
            except KeyboardInterrupt:
                return
