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

# A decorator to specify a bunch of exceptions that should be caught
# and the job retried. It turns out this comes up with relative frequency
def retry(*exceptions):
    def decorator(f):
        def _f(job):
            try:
                f(job)
            except tuple(exceptions):
                job.retry()
        return _f
    return decorator

class Jobs(object):
    def __init__(self, client):
        self.client = client
    
    def complete(self, offset=0, count=25):
        '''Return the paginated jids of complete jobs'''
        return self.client._jobs([], ['complete', offset, count])
    
    def tracked(self):
        '''Return an array of job objects that are being tracked'''
        results = json.loads(self.client._track([], []))
        results['jobs'] = [Job(self, **j) for j in results['jobs']]
        return results
    
    def tagged(self, tag, offset=0, count=25):
        '''Return the paginated jids of jobs tagged with a tag'''
        return json.loads(self.client._tag([], ['get', tag, offset, count]))
    
    def failed(self, group=None, start=0, limit=25):
        '''If no group is provided, this returns a JSON blob of the counts of the various
        types of failures known. If a type is provided, returns paginated job objects 
        affected by that kind of failure.'''
        if not group:
            return json.loads(self.client._failed([], []))
        else:
            results = json.loads(self.client._failed([], [group, start, limit]))
            results['jobs'] = [Job(self.client, **j) for j in results['jobs']]
            return results
    
    def __getitem__(self, id):
        '''Get a job object corresponding to that jid, or ``None`` if it doesn't exist'''
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
    
    def __getattr__(self, attr):
        '''What workers are workers, and how many jobs are they running'''
        if attr == 'counts':
            return json.loads(self.client._workers([], [time.time()]))
        raise AttributeError('qless.Workers has no attribute %s' % attr)
    
    def __getitem__(self, worker_name):
        '''Which jobs does a particular worker have running'''
        result = json.loads(self.client._workers([], [time.time(), worker_name]))
        result['jobs']    = result['jobs'] or []
        result['stalled'] = result['stalled'] or []
        return result

class Queues(object):
    def __init__(self, client):
        self.client = client
    
    def __getattr__(self, attr):
        '''What queues are there, and how many jobs do they have running, waiting, 
        scheduled, etc.'''
        if attr == 'counts':
            return json.loads(self.client._queues([], [time.time()]))
        raise AttributeError('qless.Queues has no attribute %s' % attr)
    
    def __getitem__(self, queue_name):
        '''Get a queue object associated with the provided queue name'''
        return Queue(queue_name, self.client, self.client.worker_name)

class Events(object):
    def __init__(self, client):
        self.pubsub    = client.redis.pubsub()
        self.callbacks = dict(
            (k, None) for k in ('canceled', 'completed', 'failed', 'popped', 'stalled', 'put', 'track', 'untrack')
        )
        r = [self.pubsub.subscribe(k) for k in self.callbacks.keys()]
    
    def next(self):
        message = self.pubsub.listen().next()
        f = (message and message['type'] == 'message' and self.callbacks.get(message['channel']))
        if f:
            f(message['data'])
    
    def listen(self):
        try:
            while True:
                self.next()
        except redis.ConnectionError:
            return
    
    def on(self, evt, f):
        if evt not in self.callbacks:
            raise NotImplementedError('callback "%s"' % evt)
        else:
            self.callbacks[evt] = f
    
    def off(self, evt):
        return self.callbacks.pop(evt, None)

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
        self.events  = Events(self)
        self.config  = Config(self)
        self.jobs    = Jobs(self)
        self.workers = Workers(self)
        self.queues  = Queues(self)
        # Client's lua scripts
        for cmd in [
            'cancel', 'config', 'complete', 'depends', 'fail', 'failed', 'get', 'heartbeat', 'jobs', 'peek',
            'pop', 'priority', 'put', 'queues', 'recur', 'retry', 'stats', 'tag', 'track', 'workers']:
            setattr(self, '_%s' % cmd, lua(cmd, self.redis))
    
    def track(self, jid):
        '''Begin tracking this job'''
        return self._track([], ['track', jid, repr(time.time())])
    
    def untrack(self, jid):
        '''Stop tracking this job'''
        return self._track([], ['untrack', jid, repr(time.time())])
    
    def tags(self, offset=0, count=100):
        '''The most common tags among jobs'''
        return json.loads(self._tag([], ['top', offset, count]))
    
    def event(self):
        '''Listen for a single event'''
        pass
    
    def events(self, *args, **kwargs):
        '''Listen indefinitely for all events'''
        while True:
            self.event(*args, **kwargs)

from lua import lua
from job import Job, RecurringJob
from queue import Queue
from config import Config