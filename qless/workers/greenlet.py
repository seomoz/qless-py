'''A Gevent-based worker'''

import qless
import gevent
import gevent.pool

from . import Worker
from qless import logger


class GeventWorker(Worker):
    '''A Gevent-based worker'''
    def __init__(self, *args, **kwargs):
        # Should we shut down after this?
        self.shutdown = False
        # A mapping of jids to the greenlets handling them
        self.greenlets = {}
        self.pool = gevent.pool.Pool(kwargs.pop('pool_size', 10))
        Worker.__init__(self, *args, **kwargs)

    def process(self, job):
        '''Process a job'''
        try:
            job.process()
        finally:
            # Delete its entry from our greenlets mapping
            self.greenlets.pop(job.jid)

    def kill(self, jid):
        '''Stop the greenlet processing the provided jid'''
        greenlet = self.greenlets.get(jid)
        if not greenlet:
            logger.warn('Worker for %s already dead' % jid)
        else:
            greenlet.kill()

    @classmethod
    def patch(cls):  # pragma: no cover
        # Monkey-patch anything that needs to be patched
        from gevent import monkey
        monkey.patch_all()

    def run(self):
        '''Work on jobs'''
        # We should probably open up our own redis client
        self.client = qless.client(self.host, self.port)
        self.queues = [self.client.queues[q] for q in self.queues]

        # And monkey-patch before doing any imports
        self.patch()

        # Start listening
        gevent.spawn(self.listen)
        try:
            generator = self.jobs()
            while not self.shutdown:
                self.pool.wait_available()
                job = generator.next()
                if job:
                    # For whatever reason, doing imports within a greenlet
                    # (there's one implicitly invoked in job.process), was
                    # throwing exceptions. The hacky way to get around this
                    # is to force the import to happen before the greenlet
                    # is spawned.
                    job.klass
                    greenlet = gevent.Greenlet(self.process, job)
                    self.greenlets[job.jid] = greenlet
                    self.pool.start(greenlet)
                else:
                    logger.debug('Sleeping for %fs' % self.interval)
                    gevent.sleep(self.interval)
        except StopIteration:  # pragma: no cover
            # It is a known bug that coverage doesn't always play nice with
            # gevent, but this code path is exercised, I promise.
            logger.info('Exhausted jobs')
        finally:  # pragma: no cover
            # It is a known bug that coverage doesn't always play nice with
            # gevent, but this code path is exercised, I promise.
            logger.info('Waiting for greenlets to finish')
            self.pool.join()
