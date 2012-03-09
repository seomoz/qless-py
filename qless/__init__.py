#! /usr/bin/env python

import os
import time
import uuid
import redis
import pkgutil
import logging
import simplejson as json

logger = logging.getLogger('qless')

class lua(object):
    def __init__(self, name, r):
        self.name  = name
        self.redis = r
        self.sha   = None
    
    def reload(self):
        data = pkgutil.get_data('qless', 'qless-core/' + self.name + '.lua')
        self.sha = self.redis.execute_command('script', 'load', data)
        logger.debug('Loaded script ' + self.sha)        
    
    def __call__(self, keys, args):
        if self.sha == None:
            self.reload()
        try:
            return self.redis.execute_command('evalsha', self.sha, len(keys), *(keys + args))
        except Exception as e:
            self.reload()
            return self.
            redis.execute_command('evalsha', self.sha, len(keys), *(keys + args))

class Stats(object):
    def __init__(self, r):
        self._stats  = lua('stats' , r)
        self._failed = lua('failed', r)
        self.redis   = r
    
    def get(self, queue, date):
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
        return json.loads(self._stats([], [queue, date]))
    
    def failed(self, t=None, start=0, limit=25):
        '''Failed(0, [type, [start, [limit]]])
        -----------------------------------
        If no type is provided, this returns a JSON blob of the counts of the various
        types of failures known. If a type is provided, it will report up to `limit`
        from `start` of the jobs affected by that issue. __Returns__ a JSON blob.'''
        if not t:
            return json.loads(self._failed([], []))
        else:
            return json.loads(self._failed([], [t, start, limit]))

class Config(object):
    def __init__(self, r):
        self._get  = lua('getconfig', r)
        self._set  = lua('setconfig', r)
        self.redis = r
    
    def get(self, option=None):
        '''GetConfig(0, [option])
        ----------------------
        Get the current configuration value for that option, or if option is omitted,
        then get all the configuration values.'''
        if option:
            return self._get([], [option])
        else:
            # This is taken from redis-py:redis/client.py
            from itertools import izip
            it = iter(self._get([], []))
            return dict(izip(it, it))
    
    def set(option, value=None):
        '''SetConfig(0, option, [value])
        -----------------------------
        Set the configuration value for the provided option. If `value` is omitted,
        then it will remove that configuration option.'''
        if value:
            return self._set([], [option, value])
        else:
            return self._set([], [option])

class Queue(object):
    def __init__(self, name, r, worker):
        self.name    = name
        self.redis   = r
        self._worker = worker
        self._hb     = 60
        # Our lua functions
        self._put       = lua('put'      , self.redis)
        self._pop       = lua('pop'      , self.redis)
        self._fail      = lua('fail'     , self.redis)
        self._peek      = lua('peek'     , self.redis)
        self._complete  = lua('complete' , self.redis)
        self._heartbeat = lua('heartbeat', self.redis)
    
    def put(self, data, priority=None, tags=None, delay=None):
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
        return self._put([self.name], [
            uuid.uuid1().hex,
            json.dumps(data),
            time.time(),
            priority or 0,
            json.dumps(tags or []),
            delay or 0
        ])
    
    def pop(self, count=None):
        '''Pop(1, queue, worker, count, now, expiration)
        ---------------------------------------------
        Passing in the queue from which to pull items, the current time, when the locks
        for these returned items should expire, and the number of items to be popped
        off.'''
        results = [Job(**json.loads(j)) for j in self._pop([self.name], [self.worker, count or 1, time.time(), time.time() + self._hb])]
        if count == None:
            return (len(results) and results[0]) or None
        return results
    
    def peek(self, count=None):
        '''Peek(1, queue, count, now)
        --------------------------
        Similar to the `Pop` command, except that it merely peeks at the next items
        in the queue.'''
        results = [Job(**json.loads(r)) for r in self._peek([self.name], [count or 1, time.time()])]
        if count == None:
            return (len(results) and results[0]) or None
        return results
    
    def fail(self, job, t, message):
        '''Fail(0, id, worker, type, message, now, [data])
        -----------------------------------------------
        Mark the particular job as failed, with the provided type, and a more specific
        message. By `type`, we mean some phrase that might be one of several categorical
        modes of failure. The `message` is something more job-specific, like perhaps
        a traceback.
        
        This method should __not__ be used to note that a job has been dropped or has 
        failed in a transient way. This method __should__ be used to note that a job has
        something really wrong with it that must be remedied.
        
        The motivation behind the `type` is so that similar errors can be grouped together.
        Optionally, updated data can be provided for the job. A job in any state can be
        marked as failed. If it has been given to a worker as a job, then its subsequent
        requests to heartbeat or complete that job will fail. Failed jobs are kept until
        they are canceled or completed. __Returns__ the id of the failed job if successful,
        or `False` on failure.'''
        return self._fail([], [job.id, self.worker, t, message, time.time(), json.dumps(job.data)]) or False
    
    def heartbeat(self, job):
        '''Heartbeat(0, id, worker, expiration, [data])
        -------------------------------------------
        Renew the heartbeat, if possible, and optionally update the job's user data.'''
        return float(self._heartbeat([], [job.id, self.worker, time.time() + self._hb, json.dumps(job.data)]) or 0)
    
    def complete(self, job, next=None, delay=None):
        '''Complete(0, id, worker, queue, now, [data, [next, [delay]]])
        -----------------------------------------------
        Complete a job and optionally put it in another queue, either scheduled or to
        be considered waiting immediately.'''
        if next:
            return self._complete([], [job.id, self.worker, self.name,
                time.time(), json.dumps(job.data), next, delay or 0]) or False
        else:
            return self._complete([], [job.id, self.worker, self.name,
                time.time(), json.dumps(job.data)]) or False
    
    def __len__(self):
        with r.pipeline() as p:
            o = p.zcard('ql:q:' + self.name + '-locks')
            o = p.zcard('ql:q:' + self.name + '-work')
            o = p.zcard('ql:q:' + self.name + '-scheduled')
            return sum(p.execute())

class Job(object):
    def __init__(self, r, id, data, priority, tags, worker, expires, state, queue, history=[]):
        # The redis instance this job is associated with
        self.redis    = r
        # The actual meat and potatoes of the job
        self.id       = id
        self.data     = data or {}
        self.priority = priority
        self.tags     = tags or []
        self.worker   = worker
        self.expires  = expires
        self.state    = state
        self.queue    = queue
        self.history  = history or []
        # Our lua scripts
        self._put     = lua('put'   , self.redis)
        self._cancel  = lua('cancel', self.redis)
    
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
    
    def remaining(self):
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
        return self._put([queue], [
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
        return self._cancel([], [self.id])

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
        self.stats  = Stats(self.redis)
        self.config = Config(self.redis)
        # Client's lua scripts
        self._get   = lua('get', self.redis)
    
    def queue(self, name):
        return Queue(name, self.redis, self.worker)
    
    def job(self, id):
        '''Get(0, id)
        ----------
        Get the data associated with a job'''
        results = self._get([], [id])
        if not results:
            return None
        return Job(self.redis, **json.loads(results))

    