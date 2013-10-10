#! /usr/bin/env python

'''Our base worker'''

import os
import code
import signal
import shutil
import itertools
import traceback
import threading
from contextlib import contextmanager

# Internal imports
from qless.listener import Listener
from qless import logger, exceptions

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
    def title(cls, message=None):
        '''Set the title of the process'''
        if message == None:
            return getproctitle()
        else:
            setproctitle('qless-py-worker %s' % message)
            logger.info(message)

    @classmethod
    def divide(cls, jobs, count):
        '''Divide up the provided jobs into count evenly-sized groups'''
        jobs = list(zip(*itertools.izip_longest(*[iter(jobs)] * count)))
        # If we had no jobs to resume, then we get an empty list
        jobs = jobs or [()] * count
        for index in xrange(count):
            # Filter out the items in jobs that are Nones
            jobs[index] = [j for j in jobs[index] if j != None]
        return jobs

    @classmethod
    def clean(cls, path):
        '''Clean up all the files in a provided path'''
        for pth in os.listdir(path):
            pth = os.path.abspath(os.path.join(path, pth))
            if os.path.isdir(pth):
                logger.debug('Removing directory %s' % pth)
                shutil.rmtree(pth)
            else:
                logger.debug('Removing file %s' % pth)
                os.remove(pth)

    @classmethod
    @contextmanager
    def sandbox(cls, path):
        '''Ensures path exists before yielding, cleans up after'''
        # Ensure the path exists and is clean
        if not os.path.exists(path):
            logger.debug('Making %s' % path)
            os.makedirs(path)
        cls.clean(path)
        # Then yield, but make sure to clean up the directory afterwards
        try:
            yield
        finally:
            cls.clean(path)

    def __init__(self, queues, client, **kwargs):
        self.client = client
        # This should accept either queue objects, or string queue names
        self.queues = []
        for queue in queues:
            if isinstance(queue, basestring):
                self.queues.append(self.client.queues[queue])
            else:
                self.queues.append(queue)

        # Save our kwargs, since a common pattern to instantiate subworkers
        self.kwargs = kwargs
        # Check for any jobs that we should resume. If 'resume' is the actual
        # value 'True', we should find all the resumable jobs we can. Otherwise,
        # we should interpret it as a list of jobs already
        self.resume = kwargs.get('resume') or []
        if self.resume == True:
            self.resume = self.resumable()
        # How frequently we should poll for work
        self.interval = kwargs.get('interval', 60)
        # To mark whether or not we should shutdown after work is done
        self.shutdown = False

    def resumable(self):
        '''Find all the jobs that we'd previously been working on'''
        # First, find the jids of all the jobs registered to this client.
        # Then, get the corresponding job objects
        jids = self.client.workers[self.client.worker_name]['jobs']
        jobs = self.client.jobs.get(*jids)

        # We'll filter out all the jobs that aren't in any of the queues
        # we're working on.
        queue_names = set([queue.name for queue in self.queues])
        return [job for job in jobs if job.queue_name in queue_names]

    def jobs(self):
        '''Generator for all the jobs'''
        # If we should resume work, then we should hand those out first,
        # assuming we can still heartbeat them
        for job in self.resume:
            try:
                if job.heartbeat():
                    yield job
            except exceptions.LostLockException:
                logger.exception('Cannot resume %s' % job.jid)
        while True:
            seen = False
            for queue in self.queues:
                job = queue.pop()
                if job:
                    seen = True
                    yield job
            if not seen:
                yield None

    @contextmanager
    def listener(self):
        '''Listen for pubsub messages relevant to this worker in a thread'''
        channels = ['ql:w:' + self.client.worker_name]
        listener = Listener(self.client.redis, channels)
        print 'Self: %s' % self
        thread = threading.Thread(target=self.listen, args=(listener,))
        thread.start()
        try:
            yield
        finally:
            listener.unlisten()
            thread.join()

    def listen(self, listener):
        '''Listen for events that affect our ownership of a job'''
        for message in listener.listen():
            try:
                data = json.loads(message['data'])
                if data['event'] in ('canceled', 'lock_lost', 'put'):
                    self.kill(data['jid'])
            except:
                logger.exception('Pubsub error')

    def kill(self, jid):
        '''Stop processing the provided jid'''
        raise NotImplementedError('Derived classes must override "kill"')

    def signals(self, signals=('QUIT', 'USR1', 'USR2')):
        '''Register our signal handler'''
        for sig in signals:
            signal.signal(getattr(signal, 'SIG' + sig), self.handler)

    def stop(self):
        '''Mark this for shutdown'''
        self.shutdown = True

    # Unfortunately, for most of this, it's not really practical to unit test
    def handler(self, signum, frame):  # pragma: no cover
        '''Signal handler for this process'''
        if signum == signal.SIGQUIT:
            # QUIT - Finish processing, but don't do any more work after that
            self.stop()
        elif signum == signal.SIGUSR1:
            # USR1 - Print the backtrace
            message = ''.join(traceback.format_stack(frame))
            message = 'Signaled traceback for %s:\n%s' % (os.getpid(), message)
            print message
            logger.warn(message)
        elif signum == signal.SIGUSR2:
            # USR2 - Enter a debugger
            # Much thanks to http://stackoverflow.com/questions/132058
            data = {'_frame': frame}    # Allow access to frame object.
            data.update(frame.f_globals)  # Unless shadowed by global
            data.update(frame.f_locals)
            # Build up a message with a traceback
            message = ''.join(traceback.format_stack(frame))
            message = 'Traceback:\n%s' % message
            code.InteractiveConsole(data).interact(message)
