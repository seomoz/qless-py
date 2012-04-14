#! /usr/bin/env python

import time
import uuid
from job import Job
import simplejson as json

# The Queue class
class Queue(object):
    def __init__(self, name, client, worker):
        self.name    = name
        self.client  = client
        self.worker  = worker
        self._hb     = 60
    
    def put(self, klass, data, priority=None, tags=None, delay=None, retries=None, jid=None, depends=None):
        # '''Put(1, queue, id, data, now, [priority, [tags, [delay, [retries]]]])
        # -----------------------------------------------------------------------
        # Either create a new job in the provided queue with the provided attributes,
        # or move that job into that queue. If the job is being serviced by a worker,
        # subsequent attempts by that worker to either `heartbeat` or `complete` the
        # job should fail and return `false`.
        #     
        # The `priority` argument should be negative to be run sooner rather than 
        # later, and positive if it's less important. The `tags` argument should be
        # a JSON array of the tags associated with the instance and the `valid after`
        # argument should be in how many seconds the instance should be considered 
        # actionable.'''
        return self.client._put([self.name], [
            jid or uuid.uuid4().hex,
            klass.__module__ + '.' + klass.__name__,
            json.dumps(data),
            repr(time.time()),
            delay or 0,
            'priority', priority or 0,
            'tags', json.dumps(tags or []),
            'retries', retries or 5,
            'depends', json.dumps(depends or [])
        ])
    
    def pop(self, count=None):
        '''Pop(1, queue, worker, count, now, expiration)
        ---------------------------------------------
        Passing in the queue from which to pull items, the current time, when the locks
        for these returned items should expire, and the number of items to be popped
        off.'''
        results = [Job(self.client, **json.loads(j)) for j in self.client._pop([self.name], [self.worker, count or 1, repr(time.time())])]
        if count == None:
            return (len(results) and results[0]) or None
        return results
    
    def peek(self, count=None):
        '''Peek(1, queue, count, now)
        --------------------------
        Similar to the `Pop` command, except that it merely peeks at the next items
        in the queue.'''
        results = [Job(self.client, **json.loads(r)) for r in self.client._peek([self.name], [count or 1, repr(time.time())])]
        if count == None:
            return (len(results) and results[0]) or None
        return results
    
    def stats(self, date=None):
        '''Stats(0, queue, date)
        ---------------------
        Return the current statistics for a given queue on a given date. The results 
        are returned are a JSON blob:
        
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
    
    def running(self):
        return self.client._jobs([], ['running', repr(time.time()), self.name])
    
    def stalled(self):
        return self.client._jobs([], ['stalled', repr(time.time()), self.name])
    
    def scheduled(self):
        return self.client._jobs([], ['scheduled', repr(time.time()), self.name])
    
    def depends(self):
        return self.client._jobs([], ['depends', repr(time.time()), self.name])
    
    def __len__(self):
        with self.client.redis.pipeline() as p:
            o = p.zcard('ql:q:' + self.name + '-locks')
            o = p.zcard('ql:q:' + self.name + '-work')
            o = p.zcard('ql:q:' + self.name + '-scheduled')
            return sum(p.execute())
