#! /usr/bin/env python

import os
import imp
import uuid
import time
import types
import traceback
from qless import logger
import simplejson as json

# The Job class
class Job(object):
    # This is a dictionary of all the classes that we've seen, and
    # the last load time for each of them. We'll use this either for
    # the debug mode or the general mechanism
    _loaded = {}
    
    @staticmethod
    def _import(klass):
        # 1) Get a reference to the module
        # 2) Check the file that module's imported from
        # 3) If that file's been updated, force a reload of that module, return it
        mod = __import__(klass.rpartition('.')[0])
        for m in klass.split('.')[1:-1]:
            mod = getattr(mod, m)
        
        # Alright, now check the file associated with it
        mtime = os.stat(mod.__file__).st_mtime
        Job._loaded[klass] = min(Job._loaded.get(klass, 1e20), time.time())
        if Job._loaded[klass] < mtime:
            mod = reload(mod)
        
        return getattr(mod, klass.rpartition('.')[2])
    
    def __setattr__(self, key, value):
        if key == 'priority':
            if self.client._priority([], [self.jid, value]) != None:
                object.__setattr__(self, key, value)
        else:
            object.__setattr__(self, key, value)
    
    def __init__(self, client, **kwargs):
        self.client = client
        for att in ['data', 'jid', 'klass', 'priority', 'tags', 'worker', 'expires', 'state', 'tracked',
        'queue', 'retries', 'remaining', 'failure', 'history', 'dependents', 'dependencies']:
            object.__setattr__(self, att, kwargs[att])
        
        # Because of how Lua parses JSON, empty tags comes through as {}
        self.tags         = self.tags         or []
        self.dependents   = self.dependents   or []
        self.dependencies = self.dependencies or []
    
    def __getitem__(self, key):
        return self.data.get(key)
    
    def __setitem__(self, key, value):
        self.data[key] = value
    
    def __str__(self):
        import pprint
        s  = 'qless:Job : %s\n' % self.jid
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
        return '<%s %s>' % (self.klass, self.jid)
    
    def module(self):
        # Return a reference to the class associated with this job. This is meant
        # as a convenience in case users ever want to instantiate the class.
        return Job._import(self.klass)
    
    def process(self):
        mod = self.module()
        # Based on the queue that this was in, we should call the appropriate
        # method. So if it was in the 'testing' queue, we should call 'testing'
        # If it doesn't have the appropriate function, we'll call process on it
        method = getattr(mod, self.queue, getattr(mod, 'process', None))
        if method:
            if isinstance(method, types.FunctionType):
                try:
                    method(self)
                except Exception as e:
                    # Make error type based on exception type
                    logger.exception('Failed %s in %s: %s' % (self.jid, self.queue, repr(method)))
                    self.fail(self.queue + '-' + e.__class__.__name__, traceback.format_exc())
            else:
                # Or fail with a message to that effect
                logger.error('Failed %s in %s : %s is not static' % (self.jid, self.queue, repr(method)))
                self.fail(self.queue + '-method-type', repr(method) + ' is not static')
        else:
            # Or fail with a message to that effect
            logger.error('Failed %s : %s is missing a method "%s" or "process"' % (self.jid, self.klass, self.queue))
            self.fail(self.queue + '-method-missing', self.klass + ' is missing a method "' + self.queue + '" or "process"')
    
    def ttl(self):
        '''How long until this expires, in seconds'''
        return self.expires - time.time()
    
    def move(self, queue, delay=0, depends=None):
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
        logger.info('Moving %s to %s from %s' % (self.jid, queue, self.queue))
        return self.client._put([queue], [
            self.jid,
            self.klass,
            json.dumps(self.data),
            repr(time.time()),
            delay,
            'depends', json.dumps(depends or [])
        ])
    
    def complete(self, next=None, delay=None, depends=None):
        '''Complete(0, id, worker, queue, now, [data, [next, [delay]]])
        -----------------------------------------------
        Complete a job and optionally put it in another queue, either scheduled or to
        be considered waiting immediately.'''
        if next:
            logger.info('Advancing %s to %s from %s' % (self.jid, next, self.queue))
            return self.client._complete([], [self.jid, self.client.worker, self.queue,
                repr(time.time()), json.dumps(self.data), 'next', next, 'delay', delay or 0,
                'depends', json.dumps(depends or [])]) or False
        else:
            logger.info('Completing %s' % self.jid)
            return self.client._complete([], [self.jid, self.client.worker, self.queue,
                repr(time.time()), json.dumps(self.data)]) or False
    
    def heartbeat(self):
        '''Heartbeat(0, id, worker, expiration, [data])
        -------------------------------------------
        Renew the heartbeat, if possible, and optionally update the job's user data.'''
        self.expires = float(self.client._heartbeat([], [self.jid, self.client.worker, repr(time.time()), json.dumps(self.data)]) or 0)
        return self.expires
    
    def fail(self, group, message):
        '''Fail(0, id, worker, group, message, now, [data])
        ---------------------------------------------------
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
        logger.warn('Failing %s (%s): %s' % (self.jid, group, message))
        return self.client._fail([], [self.jid, self.client.worker, group, message, repr(time.time()), json.dumps(self.data)]) or False
    
    def cancel(self):
        '''Cancel(0, id)
        -------------
        Cancel a job from taking place. It will be deleted from the system, and any
        attempts to renew a heartbeat will fail, and any attempts to complete it
        will fail. If you try to get the data on the object, you will get nothing.'''
        return self.client._cancel([], [self.jid])
    
    def track(self):
        return self.client._track([], ['track', self.jid, repr(time.time())])
    
    def untrack(self):
        return self.client._track([], ['untrack', self.jid, repr(time.time())])
    
    def tag(self, *tags):
        args = ['add', self.jid, repr(time.time())]
        args.extend(tags)
        return self.client._tag([], args)
    
    def untag(self, *tags):
        args = ['remove', self.jid, repr(time.time())]
        args.extend(tags)
        return self.client._tag([], args)
    
    def retry(self, delay=0):
        result = self.client._retry([], [self.jid, self.queue, self.worker, repr(time.time()), delay])
        if result == None:
            return False
        return result
    
    def depend(self, *args):
        # Depends(0, jid, ('on', [jid, [jid, [...]]]) | ('off', ('all' | [jid, [jid, [...]]]))
        # ------------------------------------------------------------------------------------
        # Add or remove dependencies a job has. If 'on' is provided, the provided jids are 
        # added as dependencies. If 'off' and 'all' are provided, then all the current dependencies
        # are removed. If 'off' is provided and the next argument is not 'all', then those
        # jids are removed as dependencies.
        # 
        # If a job is not already in the 'depends' state, then this call will return false.
        # Otherwise, it will return true
        return self.client._depends([], [self.jid, 'on'] + list(args)) or False
    
    def undepend(self, *args, **kwargs):
        if kwargs.get('all', False):
            return self.client._depends([], [self.jid, 'off', 'all']) or False
        else:
            return self.client._depends([], [self.jid, 'off'] + list(args)) or False