#! /usr/bin/env python

'''Our base worker'''

import os
import redis
import qless
import shutil
import psutil
from qless import logger

# Try to use the fast json parser
try:
    import simplejson as json
except ImportError:  # pragma: no cover
    import json

# Setting the process title
try:
    from setproctitle import setproctitle, getproctitle
except ImportError:  # pragma: no cover
    def setproctitle(title):
        pass

    def getproctitle():
        return ''


class Worker(object):
    '''Worker. For doing work'''
    @classmethod
    def clean(cls, sandbox):
        '''Clean up the sandbox of all files'''
        # And that it's clear of any files
        for path in os.listdir(sandbox):
            path = os.path.join(sandbox, path)
            if os.path.isdir(path):
                logger.info('Removing tree %s...' % path)
                shutil.rmtree(path)
            else:
                logger.info('Removing file %s...' % path)
                os.remove(path)

    @classmethod
    def title(cls, message=None):
        '''Set the title of the process'''
        if message == None:
            return getproctitle()
        else:
            setproctitle('qless-py-worker %s' % message)
            logger.info(message)

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
        # This is for filesystem sandboxing. Each worker has
        # a directory associated with it, which it should make
        # sure is the working directory in which it runs each
        # of the jobs. It should also ensure that the directory
        # exists, and clobbers files before each run, and after.
        self.workdir = os.path.abspath(workdir)
        # These are the job ids that I should get to first, before
        # picking off other jobs
        self.jids = {}

    def jobs(self):
        '''Generator for all the jobs'''
        while True:
            seen = False
            for queue in self.queues:
                job = queue.pop()
                if job:
                    seen = True
                    yield job
            if not seen:
                yield None

    def reconnect(self):
        '''Connect to the provided instance'''
        self.client = qless.client(self.host, self.port)
        self.queues = [self.client.queues[q] for q in self.queues]

    def listen(self):
        '''Listen for pubsub messages relevant to this worker'''
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
                self.kill(jid)

    def kill(self, jid):
        '''Stop processing the provided jid'''
        raise NotImplementedError('Derived classes must override "kill"')
