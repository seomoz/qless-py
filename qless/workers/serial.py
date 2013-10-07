'''A worker that serially pops and complete jobs'''

import time
import threading
from qless import logger

from . import Worker


class SerialWorker(Worker):
    '''A worker that just does serial work'''
    def __init__(self, *args, **kwargs):
        Worker.__init__(self, *args, **kwargs)
        # The jid that we're working on at the moment
        self.jid = None

    def kill(self, jid):
        '''The best way to do this is to fall on our sword'''
        if jid == self.jid:
            exit(1)

    def run(self):
        '''Run jobs, popping one after another'''
        # Register our signal handlers
        self.signals()

        thread = threading.Thread(target=self.listen)
        try:
            thread.start()
            for job in self.jobs():
                # If there was no job to be had, we should sleep a little bit
                if not job:
                    self.jid = None
                    self.title('Sleeping for %fs' % self.interval)
                    time.sleep(self.interval)
                else:
                    self.jid = job.jid
                    self.title('Working on %s (%s)' % (job.jid, job.klass_name))
                    job.process()
                if self.shutdown:
                    break
        finally:
            thread.join()
