#! /usr/bin/env python

'''Our base worker'''

from qless import logger

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
        # Check for any jobs that we should resume
        self.resume = kwargs.get('resume', [])
        # How frequently we should poll for work
        self.interval = kwargs.get('interval', 60)

    def jobs(self):
        '''Generator for all the jobs'''
        # If we should resume work, then we should hand those out first,
        # assuming we can still heartbeat them
        for jid in self.resume:
            try:
                job = self.client.jobs[jid]
                if job.heartbeat():
                    yield job
            except:
                logger.exception('Cannot resume %s' % jid)
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
