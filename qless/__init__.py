#! /usr/bin/env python

import os
import time
import redis
import logging
import simplejson as json

logger = logging.getLogger('qless')
formatter = logging.Formatter('%(asctime)s | PID %(process)d | [%(levelname)s] %(message)s')
handler = logging.StreamHandler()
handler.setLevel(logging.DEBUG)
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.FATAL)

class Jobs(object):
    def __init__(self, client):
        self.client = client
    
    def complete(self, offset=0, count=25):
        return self.client._jobs([], ['complete', offset, count])
    
    def tracked(self):
        results = json.loads(self.client._track([], []))
        results['jobs'] = [Job(self, **j) for j in results['jobs']]
        return results
    
    def tagged(self, tag, offset=0, count=25):
        return json.loads(self.client._tag([], ['get', tag, offset, count]))
    
    def failed(self, group=None, start=0, limit=25):
        '''Failed(0, [group, [start, [limit]]])
        ---------------------------------------
        If no type is provided, this returns a JSON blob of the counts of the various
        types of failures known. If a type is provided, it will report up to `limit`
        from `start` of the jobs affected by that issue. __Returns__ a JSON blob.'''
        if not group:
            return json.loads(self.client._failed([], []))
        else:
            results = json.loads(self.client._failed([], [group, start, limit]))
            results['jobs'] = [Job(self.client, **j) for j in results['jobs']]
            return results
    
    def __getitem__(self, id):
        '''Get(0, id)
        ----------
        Get the data associated with a job'''
        results = self.client._get([], [id])
        if not results:
            results = self.client._recur([], ['get', id])
            if not results:
                return None
            return RecurringJob(self.client, **json.loads(results))
        return Job(self.client, **json.loads(results))

class Workers(object):
    def __init__(self, client):
        self.client = client
    
    def all(self):
        return json.loads(self.client._workers([], [time.time()]))
    
    def __getitem__(self, worker_name):
        result = json.loads(self.client._workers([], [time.time(), worker_name]))
        result['jobs']    = result['jobs'] or []
        result['stalled'] = result['stalled'] or []
        return result

class Queues(object):
    def __init__(self, client):
        self.client = client
    
    def __getattr__(self, attr):
        if attr == 'counts':
            return json.loads(self.client._queues([], [time.time()]))
        raise AttributeError('qless.Queues has no attribute %s' % attr)
    
    def __getitem__(self, queue_name):
        return Queue(queue_name, self.client, self.client.worker_name)

class client(object):
    def __init__(self, host='localhost', port=6379, hostname = None, **kwargs):
        import os
        import socket
        # This is our unique idenitifier as a worker
        self.worker_name = hostname or socket.gethostname()
        # This is just the redis instance we're connected to
        # conceivably someone might want to work with multiple
        # instances simultaneously.
        self.redis   = redis.Redis(host, port, **kwargs)
        self.config  = Config(self)
        self.jobs    = Jobs(self)
        self.workers = Workers(self)
        self.queues  = Queues(self)
        # Client's lua scripts
        for cmd in [
            'cancel', 'complete', 'depends', 'fail', 'failed', 'get', 'getconfig', 'heartbeat', 'jobs', 'peek',
            'pop', 'priority', 'put', 'queues', 'recur', 'retry', 'setconfig', 'stats', 'tag', 'track', 'workers']:
            setattr(self, '_%s' % cmd, lua(cmd, self.redis))
    
    def tags(self, offset=0, count=100):
        return json.loads(self._tag([], ['top', offset, count]))

from lua import lua
from job import Job, RecurringJob
from queue import Queue
from config import Config