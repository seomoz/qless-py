#! /usr/bin/env python

import os
import time
import redis
import logging
import simplejson as json

logger = logging.getLogger('qless')

class client(object):
    def __init__(self, *args, **kwargs):
        import os
        import socket
        # This is our unique idenitifier as a worker
        self.worker = socket.gethostname() + '-' + str(os.getpid())
        # This is just the redis instance we're connected to
        # conceivably someone might want to work with multiple
        # instances simultaneously.
        self.redis  = redis.Redis(*args, **kwargs)
        self.config = Config(self)
        # Client's lua scripts
        for cmd in [
            'cancel', 'complete', 'fail', 'failed', 'get', 'getconfig', 'heartbeat',
            'jobs', 'peek', 'pop', 'put', 'queues', 'setconfig', 'stats', 'track', 'workers']:
            setattr(self, '_%s' % cmd, lua(cmd, self.redis))
    
    def queue(self, name):
        return Queue(name, self, self.worker)
    
    def queues(self, queue=None):
        if queue:
            return json.loads(self._queues([], [time.time(), queue]))
        return json.loads(self._queues([], [time.time()]))
    
    def tracked(self):
        results = json.loads(self._track([], []))
        results['jobs'] = [Job(self, **j) for j in results['jobs']]
        return results
    
    def failed(self, group=None, start=0, limit=25):
        '''Failed(0, [group, [start, [limit]]])
        ---------------------------------------
        If no type is provided, this returns a JSON blob of the counts of the various
        types of failures known. If a type is provided, it will report up to `limit`
        from `start` of the jobs affected by that issue. __Returns__ a JSON blob.'''
        if not group:
            return json.loads(self._failed([], []))
        else:
            results = json.loads(self._failed([], [group, start, limit]))
            results['jobs'] = [Job(self, **j) for j in results['jobs']]
            return results
    
    def workers(self, worker=None):
        if worker:
            return json.loads(self._workers([], [time.time(), worker]))
        return json.loads(self._workers([], [time.time()]))
    
    def job(self, id):
        '''Get(0, id)
        ----------
        Get the data associated with a job'''
        results = self._get([], [id])
        if not results:
            return None
        return Job(self, **json.loads(results))

from lua import lua
from job import Job
from queue import Queue
from config import Config