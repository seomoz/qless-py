#! /usr/bin/env python

import time
import qless
import random

class CrawlJob(qless.Job):
    def crawl(self):
        # We'll fail a certain portion of these jobs
        if random.random() < 0.005:
            raise Exception('Some sort of crawling error')
        else:
            time.sleep(random.random() * 0.25)
            self.data.setdefault('stages', {})
            self['stages'].setdefault('crawl', {})
            self['stages']['crawl'] = {
                'crawled': int(random.random() * 500),
                'uncrawled': 0,
                'uploads': {
                    'crawl': {
                        'bucket': 'pr1-crawl',
                        'key': self.jid + '.dmp',
                        'path': '/vol/crawl/' + self.jid + '.dmp'
                    }
                }
            }            
            self.complete('store')
    
    def store(self):
        # We'll fail a certain portion of these
        if random.random() < 0.005:
            raise Exception('Some sort of store stage error')
        else:
            time.sleep(random.random() * 0.25)
            self.data.setdefault('stages', {})
            self['stages'].setdefault('store', {})
            self['stages']['store'] = {
                'downloads': {
                    'crawl': {
                        'bucket': 'pr1-crawl',
                        'path': '/vol/download/' + self.jid + '.dmp',
                        'key': self.jid + '.dmp'
                    }
                }, 'uploads': {
                    'store': {
                        'bucket': 'pr1-store',
                        'path': '/vol/store/' + self.jid + '.csv',
                        'key': self.jid + '.csv'
                    }
                }
            }
            self.complete('issues')
    
    def issues(self):
        # We'll fail a certain portion of these
        if random.random() < 0.005:
            raise Exception('Some sort of issues stage error')
        else:
            time.sleep(random.random() * 0.25)
            self.data.setdefault('stages', {})
            self['stages'].setdefault('issues', {})
            self['stages']['issues'] = {
                'crawled': int(self['stages']['crawl']['crawled'] * 0.2),
                'downloads': {
                    'issues': {
                        'bucket': 'pr1-crawl',
                        'path': '/vol/download/' + self.jid + '.dmp',
                        'key': self.jid + '.dmp'
                    }
                }, 'uploads': {
                    'lsapi': {
                        'bucket': 'pr1-store',
                        'path': '/vol/lsapi/' + self.jid + '.csv',
                        'key': self.jid + '.json'
                    }, 'csv': {
                        'bucket': 'pr1-issues',
                        'path': '/vol/issues/' + self.jid + '.csv',
                        'key': self.jid + '.csv'
                    }
                }
            }
            self.complete('lsapi')

    def lsapi(self):
        if random.random() < 0.005:
            raise Exception('Some sort of lsapi stage error')
        else:
            time.sleep(random.random() * 0.25)
            self.data.setdefault('stages', {})
            self['stages'].setdefault('lsapi', {})
            self['stages']['lsapi'] = {
                'downloads': {
                    'lsapi': {
                        'bucket': 'pr1-lsapi',
                        'path': '/vol/download/' + self.jid + '.dmp',
                        'key': self.jid + '.dmp'
                    }
                }
            }
            self.complete()
