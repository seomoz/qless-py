#! /usr/bin/env python

import time
import uuid
from job import Job
import simplejson as json

class Jobs(object):
    def __init__(self, name, client):
        self.name   = name
        self.client = client
    
    def running(self, offset=0, count=25):
        return self.client._jobs([], ['running', repr(time.time()), self.name, offset, count])
    
    def stalled(self, offset=0, count=25):
        return self.client._jobs([], ['stalled', repr(time.time()), self.name, offset, count])
    
    def scheduled(self, offset=0, count=25):
        return self.client._jobs([], ['scheduled', repr(time.time()), self.name, offset, count])
    
    def depends(self, offset=0, count=25):
        return self.client._jobs([], ['depends', repr(time.time()), self.name, offset, count])
    
    def recurring(self, offset=0, count=25):
        return self.client._jobs([], ['recurring', repr(time.time()), self.name, offset, count])

# The Queue class
class Queue(object):
    def __init__(self, name, client, worker_name):
        self.name        = name
        self.client      = client
        self.worker_name = worker_name
        self._hb         = 60
    
    def __getattr__(self, key):
        if key == 'jobs':
            self.jobs = Jobs(self.name, self.client)
            return self.jobs
        if key == 'counts':
            return json.loads(self.client._queues([], [time.time(), self.name]))
        if key == 'heartbeat':
            a = client.config.all
            return int(a.get(self.name + '-heartbeat', a.get('heartbeat', 60)))
        raise AttributeError('qless.Queue has no attribute %s' % key)
    
    def __setattr__(self, key, value):
        if key == 'heartbeat':
            self.client.config[self.name + '-heartbeat'] = value
        else:
            object.__setattr__(self, key, value)
   
    def class_string(self, klass):
        if isinstance(klass,basestring):
            return klass
        return klass.__module__ + '.' + klass.__name__
 
    def put(self, klass, data, priority=None, tags=None, delay=None, retries=None, jid=None, depends=None):
        '''Either create a new job in the provided queue with the provided attributes,
        or move that job into that queue. If the job is being serviced by a worker,
        subsequent attempts by that worker to either `heartbeat` or `complete` the
        job should fail and return `false`.
            
        The `priority` argument should be negative to be run sooner rather than 
        later, and positive if it's less important. The `tags` argument should be
        a JSON array of the tags associated with the instance and the `valid after`
        argument should be in how many seconds the instance should be considered 
        actionable.'''
        return self.client._put([self.name], [
            jid or uuid.uuid4().hex,
            self.class_string(klass),
            json.dumps(data),
            repr(time.time()),
            delay or 0,
            'priority', priority or 0,
            'tags', json.dumps(tags or []),
            'retries', retries or 5,
            'depends', json.dumps(depends or [])
        ])
    
    def recur(self, klass, data, interval, offset=0, priority=None, tags=None, retries=None, jid=None):
        return self.client._recur([], ['on', self.name,
            jid or uuid.uuid4().hex,
            self.class_string(klass),
            json.dumps(data),
            repr(time.time()),
            'interval', interval, offset,
            'priority', priority or 0,
            'tags', json.dumps(tags or []),
            'retries', retries or 5
        ])
    
    def pop(self, count=None):
        '''Passing in the queue from which to pull items, the current time, when the locks
        for these returned items should expire, and the number of items to be popped
        off.'''
        results = [Job(self.client, **json.loads(j)) for j in self.client._pop([self.name], [self.worker_name, count or 1, repr(time.time())])]
        if count == None:
            return (len(results) and results[0]) or None
        return results
    
    def peek(self, count=None):
        '''Similar to the pop command, except that it merely peeks at the next items'''
        results = [Job(self.client, **json.loads(r)) for r in self.client._peek([self.name], [count or 1, repr(time.time())])]
        if count == None:
            return (len(results) and results[0]) or None
        return results
    
    def stats(self, date=None):
        '''Return the current statistics for a given queue on a given date. The results 
        are returned are a JSON blob::
        
            {
                'total'    : ...,
                'mean'     : ...,
                'variance' : ...,
                'histogram': [
                    ...
                ]
            }
        
        The histogram's data points are at the second resolution for the first minute,
        the minute resolution for the first hour, the 15-minute resolution for the first
        day, the hour resolution for the first 3 days, and then at the day resolution
        from there on out. The `histogram` key is a list of those values.'''
        return json.loads(self.client._stats([], [self.name, date or repr(time.time())]))
    
    def __len__(self):
        with self.client.redis.pipeline() as p:
            o = p.zcard('ql:q:' + self.name + '-locks')
            o = p.zcard('ql:q:' + self.name + '-work')
            o = p.zcard('ql:q:' + self.name + '-scheduled')
            return sum(p.execute())
