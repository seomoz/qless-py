#! /usr/bin/env python

'''Our base worker'''

import itertools
from qless import logger
from qless.exceptions import LostLockException

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
            except LostLockException:
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

    def listen(self):
        '''Listen for pubsub messages relevant to this worker'''
        logger.info('Listening on pubsub')
        pubsub = self.client.redis.pubsub()
        pubsub.subscribe(['ql:w:' + self.client.worker_name])
        for message in pubsub.listen():
            if message['type'] != 'message':
                continue

            # If anything has happened to affect our ownership of a job, we
            # should kill that worker
            data = json.loads(message['data'])
            if data['event'] in ('canceled', 'lock_lost', 'put'):
                jid = data['jid']
                logger.warn('Lost ownerhsip of job %s' % jid)
                self.kill(jid)

    def kill(self, jid):
        '''Stop processing the provided jid'''
        raise NotImplementedError('Derived classes must override "kill"')
