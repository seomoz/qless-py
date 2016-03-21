#! /usr/bin/env python

'''Both the regular Job and RecurringJob classes'''

import os
import time
import types
import traceback
import simplejson as json
from six.moves import reload_module

# Internal imports
from qless import logger
from qless.exceptions import LostLockException, QlessException


class BaseJob(object):
    '''This is a dictionary of all the classes that we've seen, and
    the last load time for each of them. We'll use this either for
    the debug mode or the general mechanism'''
    _loaded = {}

    def __init__(self, client, **kwargs):
        self.client = client
        for att in ['jid', 'priority']:
            object.__setattr__(self, att, kwargs[att])
        object.__setattr__(self, 'klass_name', kwargs['klass'])
        object.__setattr__(self, 'queue_name', kwargs['queue'])
        # Because of how Lua parses JSON, empty tags comes through as {}
        object.__setattr__(self, 'tags', kwargs['tags'] or [])
        object.__setattr__(self, 'data', json.loads(kwargs['data']))

    def __setattr__(self, key, value):
        if key == 'priority':
            return self.client('priority', self.jid, value
                ) and object.__setattr__(self, key, value)
        else:
            return object.__setattr__(self, key, value)

    def __getattr__(self, key):
        if key == 'queue':
            # An actual queue instance
            object.__setattr__(self, 'queue',
                self.client.queues[self.queue_name])
            return self.queue
        elif key == 'klass':
            # Get a reference to the provided klass
            object.__setattr__(self, 'klass', self._import(self.klass_name))
            return self.klass
        raise AttributeError('%s has no attribute %s' % (
            self.__class__.__module__ + '.' + self.__class__.__name__, key))

    @staticmethod
    def reload(klass):
        '''Force a reload of this klass on next import'''
        BaseJob._loaded[klass] = 0

    @staticmethod
    def _import(klass):
        '''1) Get a reference to the module
           2) Check the file that module's imported from
           3) If that file's been updated, force a reload of that module
                return it'''
        mod = __import__(klass.rpartition('.')[0])
        for segment in klass.split('.')[1:-1]:
            mod = getattr(mod, segment)

        # Alright, now check the file associated with it. Note that clases
        # defined in __main__ don't have a __file__ attribute
        if klass not in BaseJob._loaded:
            BaseJob._loaded[klass] = time.time()
        if hasattr(mod, '__file__'):
            try:
                mtime = os.stat(mod.__file__).st_mtime
                if BaseJob._loaded[klass] < mtime:
                    mod = reload_module(mod)
            except OSError:
                logger.warn('Could not check modification time of %s',
                    mod.__file__)

        return getattr(mod, klass.rpartition('.')[2])

    def cancel(self):
        '''Cancel a job. It will be deleted from the system, the thinking
        being that if you don't want to do any work on it, it shouldn't be in
        the queueing system.'''
        return self.client('cancel', self.jid)

    def tag(self, *tags):
        '''Tag a job with additional tags'''
        return self.client('tag', 'add', self.jid, *tags)

    def untag(self, *tags):
        '''Remove tags from a job'''
        return self.client('tag', 'remove', self.jid, *tags)


class Job(BaseJob):
    '''The Job class'''
    def __init__(self, client, **kwargs):
        BaseJob.__init__(self, client, **kwargs)
        self.client = client
        for att in ['state', 'tracked', 'failure',
            'history', 'dependents', 'dependencies']:
            object.__setattr__(self, att, kwargs[att])

        # The reason we're using object.__setattr__ directly is because
        # we have __setattr__ defined for this class, and we're actually
        # just interested in setting these memebers directly
        object.__setattr__(self, 'expires_at', kwargs['expires'])
        object.__setattr__(self, 'original_retries', kwargs['retries'])
        object.__setattr__(self, 'retries_left', kwargs['remaining'])
        object.__setattr__(self, 'worker_name', kwargs['worker'])
        # Because of how Lua parses JSON, empty lists come through as {}
        object.__setattr__(self, 'dependents', kwargs['dependents'] or [])
        object.__setattr__(self, 'dependencies', kwargs['dependencies'] or [])

    def __getattr__(self, key):
        if key == 'ttl':
            # How long until this expires, in seconds
            return self.expires_at - time.time()
        return BaseJob.__getattr__(self, key)

    def __getitem__(self, key):
        return self.data.get(key)

    def __setitem__(self, key, value):
        self.data[key] = value

    def __repr__(self):
        return '<%s %s>' % (self.klass_name, self.jid)

    def process(self):
        '''Load the module containing your class, and run the appropriate
        method. For example, if this job was popped from the queue
        ``testing``, then this would invoke the ``testing`` staticmethod of
        your class.'''
        try:
            method = getattr(self.klass, self.queue_name,
                getattr(self.klass, 'process', None))
        except Exception as exc:
            # We failed to import the module containing this class
            logger.exception('Failed to import %s', self.klass_name)
            return self.fail(self.queue_name + '-' + exc.__class__.__name__,
                'Failed to import %s' % self.klass_name)

        if method:
            if isinstance(method, types.FunctionType):
                try:
                    logger.info('Processing %s in %s',
                        self.jid, self.queue_name)
                    method(self)
                    logger.info('Completed %s in %s',
                        self.jid, self.queue_name)
                except Exception as exc:
                    # Make error type based on exception type
                    logger.exception('Failed %s in %s: %s',
                        self.jid, self.queue_name, repr(method))
                    self.fail(self.queue_name + '-' + exc.__class__.__name__,
                        traceback.format_exc())
            else:
                # Or fail with a message to that effect
                logger.error('Failed %s in %s : %s is not static',
                    self.jid, self.queue_name, repr(method))
                self.fail(self.queue_name + '-method-type',
                    repr(method) + ' is not static')
        else:
            # Or fail with a message to that effect
            logger.error('Failed %s : %s is missing a method "%s" or "process"',
                         self.jid, self.klass_name, self.queue_name)
            self.fail(self.queue_name + '-method-missing', self.klass_name +
                ' is missing a method "' + self.queue_name + '" or "process"')

    def move(self, queue, delay=0, depends=None):
        '''Move this job out of its existing state and into another queue. If
        a worker has been given this job, then that worker's attempts to
        heartbeat that job will fail. Like ``Queue.put``, this accepts a
        delay, and dependencies'''
        logger.info('Moving %s to %s from %s',
            self.jid, queue, self.queue_name)
        return self.client('put', queue, self.jid, self.klass_name,
            json.dumps(self.data), delay, 'depends', json.dumps(depends or [])
        )

    def complete(self, nextq=None, delay=None, depends=None):
        '''Turn this job in as complete, optionally advancing it to another
        queue. Like ``Queue.put`` and ``move``, it accepts a delay, and
        dependencies'''
        if nextq:
            logger.info('Advancing %s to %s from %s',
                self.jid, nextq, self.queue_name)
            return self.client('complete', self.jid, self.client.worker_name,
                self.queue_name, json.dumps(self.data), 'next', nextq,
                'delay', delay or 0, 'depends', json.dumps(depends or [])
            ) or False
        else:
            logger.info('Completing %s', self.jid)
            return self.client('complete', self.jid, self.client.worker_name,
                self.queue_name, json.dumps(self.data)) or False

    def heartbeat(self):
        '''Renew the heartbeat, if possible, and optionally update the job's
        user data.'''
        logger.debug('Heartbeating %s (ttl = %s)', self.jid, self.ttl)
        try:
            self.expires_at = float(self.client('heartbeat', self.jid,
            self.client.worker_name, json.dumps(self.data)) or 0)
        except QlessException:
            raise LostLockException(self.jid)
        logger.debug('Heartbeated %s (ttl = %s)', self.jid, self.ttl)
        return self.expires_at

    def fail(self, group, message):
        '''Mark the particular job as failed, with the provided type, and a
        more specific message. By `type`, we mean some phrase that might be
        one of several categorical modes of failure. The `message` is
        something more job-specific, like perhaps a traceback.

        This method should __not__ be used to note that a job has been dropped
        or has failed in a transient way. This method __should__ be used to
        note that a job has something really wrong with it that must be
        remedied.

        The motivation behind the `type` is so that similar errors can be
        grouped together. Optionally, updated data can be provided for the job.
        A job in any state can be marked as failed. If it has been given to a
        worker as a job, then its subsequent requests to heartbeat or complete
        that job will fail. Failed jobs are kept until they are canceled or
        completed. __Returns__ the id of the failed job if successful, or
        `False` on failure.'''
        logger.warn('Failing %s (%s): %s', self.jid, group, message)
        return self.client('fail', self.jid, self.client.worker_name, group,
            message, json.dumps(self.data)) or False

    def track(self):
        '''Begin tracking this job'''
        return self.client('track', 'track', self.jid)

    def untrack(self):
        '''Stop tracking this job'''
        return self.client('track', 'untrack', self.jid)

    def retry(self, delay=0):
        '''Retry this job in a little bit, in the same queue. This is meant
        for the times when you detect a transient failure yourself'''
        return self.client('retry', self.jid, self.queue_name,
            self.worker_name, delay)

    def depend(self, *args):
        '''If and only if a job already has other dependencies, this will add
        more jids to the list of this job's dependencies.'''
        return self.client('depends', self.jid, 'on', *args) or False

    def undepend(self, *args, **kwargs):
        '''Remove specific (or all) job dependencies from this job:

            job.remove(jid1, jid2)
            job.remove(all=True)'''
        if kwargs.get('all', False):
            return self.client('depends', self.jid, 'off', 'all') or False
        else:
            return self.client('depends', self.jid, 'off', *args) or False

    def timeout(self):
        '''Time out this job'''
        self.client('timeout', self.jid)


class RecurringJob(BaseJob):
    '''Recurring Job object'''
    def __init__(self, client, **kwargs):
        BaseJob.__init__(self, client, **kwargs)
        for att in ['jid', 'priority', 'tags',
            'retries', 'interval', 'count']:
            object.__setattr__(self, att, kwargs[att])
        object.__setattr__(self, 'client', client)
        object.__setattr__(self, 'klass_name', kwargs['klass'])
        object.__setattr__(self, 'queue_name', kwargs['queue'])
        object.__setattr__(self, 'tags', self.tags or [])
        object.__setattr__(self, 'data', json.loads(kwargs['data']))

    def __setattr__(self, key, value):
        if key in ('priority', 'retries', 'interval'):
            return self.client('recur.update', self.jid, key, value
                ) and object.__setattr__(self, key, value)
        if key == 'data':
            return self.client('recur.update', self.jid, key, json.dumps(value)
                ) and object.__setattr__(self, 'data', value)
        if key == 'klass':
            name = value.__module__ + '.' + value.__name__
            return self.client('recur.update', self.jid, 'klass', name
                ) and object.__setattr__(self, 'klass_name', name
                ) and object.__setattr__(self, 'klass', value)
        return object.__setattr__(self, key, value)

    def __getattr__(self, key):
        if key == 'next':
            # The time (seconds since epoch) until the next time it's run
            return self.client.redis.zscore(
                'ql:q:' + self.queue_name + '-recur', self.jid)
        return BaseJob.__getattr__(self, key)

    def move(self, queue):
        '''Make this recurring job attached to another queue'''
        return self.client('recur.update', self.jid, 'queue', queue)

    def cancel(self):
        '''Cancel all future recurring jobs'''
        self.client('unrecur', self.jid)

    def tag(self, *tags):
        '''Add tags to this recurring job'''
        return self.client('recur.tag', self.jid, *tags)

    def untag(self, *tags):
        '''Remove tags from this job'''
        return self.client('recur.untag', self.jid, *tags)
