#! /usr/bin/env python

import time
import random

class GnomesJob(object):
    @staticmethod
    def underpants(job):
        # Let's collect some underpants
        if random.random() < 0.005:
            raise Exception('Sometimes underpants are hard to get')
        else:
            # It takes time to collect underpants
            time.sleep(random.random() * 0.5)
            job['underpants'] = {
                'collected': random.randrange(0, 200)
            }
            job.complete('unknown')
    
    @staticmethod
    def unknown(job):
        # After all, it is the unknown stage
        if random.random() < 0.05:
            raise Exception('The most uncertain plans of mice and men...')
        else:
            # We'll scratch our heads a little bit about what to do here
            time.sleep(random.random() * 2.0)
            job['unknown'] = ['?'] * int(random.random() * 10)
            job.complete('profit')
    
    @staticmethod
    def profit(job):
        # How much profits did we get?!
        job['profit'] = '$%.2f' % (random.uniform(0.5, 1.5) * job['underpants']['collected'])
        job.complete()

if __name__ == '__main__':
    import qless
    from qless import gnomes
    client = qless.client()
    underpants = client.queue('underpants')
    for i in range(1000):
        underpants.put(gnomes.GnomesJob, {})
