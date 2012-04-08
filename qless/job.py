#! /usr/bin/env python

import time
import simplejson as json

# The Job class
class Job(object):
    def __init__(self, client, id, data, priority, tags, worker, expires, state, queue, remaining, retries, failure={}, history=[]):
        # The redis instance this job is associated with
        self.client    = client
        # The actual meat and potatoes of the job
        self.id        = id
        self.data      = data or {}
        self.priority  = priority
        self.tags      = tags or []
        self.worker    = worker
        self.expires   = expires
        self.state     = state
        self.queue     = queue
        self.retries   = retries
        self.remaining = remaining
        self.failure   = failure or {}
        self.history   = history or []
    
    def __getitem__(self, key):
        return self.data.get(key)
    
    def __setitem__(self, key, value):
        self.data[key] = value
    
    def __str__(self):
        import pprint
        s  = 'qless:Job : %s\n' % self.id
        s += '\tpriority: %i\n' % self.priority
        s += '\ttags: %s\n' % ', '.join(self.tags)
        s += '\tworker: %s\n' % self.worker
        s += '\texpires: %i\n' % self.expires
        s += '\tstate: %s\n' % self.state
        s += '\tqueue: %s\n' % self.queue
        s += '\thistory:\n'
        for h in self.history:
            s += '\t\t%s (%s)\n' % (h['queue'], h['worker'])
            s += '\t\tput: %i\n' % h['put']
            if h['popped']:
                s += '\t\tpopped: %i\n' % h['popped']
            if h['completed']:
                s += '\t\tcompleted: %i\n' % h['completed']
        s += '\tdata: %s' % pprint.pformat(self.data)
        return s
    
    def __repr__(self):
        return '<qless:Job %s>' % self.id
    
    def ttl(self):
        '''How long until this expires, in seconds'''
        return time.time() - self.expires
    
    def move(self, queue):
        '''Put(1, queue, id, data, now, [priority, [tags, [delay]]])
        ---------------------------------------------------------------    
        Either create a new job in the provided queue with the provided attributes,
        or move that job into that queue. If the job is being serviced by a worker,
        subsequent attempts by that worker to either `heartbeat` or `complete` the
        job should fail and return `false`.
        
        The `priority` argument should be negative to be run sooner rather than 
        later, and positive if it's less important. The `tags` argument should be
        a JSON array of the tags associated with the instance and the `valid after`
        argument should be in how many seconds the instance should be considered 
        actionable.'''
        return self.client._put([queue], [
            self.id,
            json.dumps(self.data),
            time.time()
        ])
    
    def cancel(self):
        '''Cancel(0, id)
        -------------
        Cancel a job from taking place. It will be deleted from the system, and any
        attempts to renew a heartbeat will fail, and any attempts to complete it
        will fail. If you try to get the data on the object, you will get nothing.'''
        return self.client._cancel([], [self.id])
    
    def track(self, *tags):
        args = ['track', self.id, time.time()]
        args.extend(tags)
        return self.client._track([], args)
    
    def untrack(self):
        return self.client._track([], ['untrack', self.id, time.time()])
