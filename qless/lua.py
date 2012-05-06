#! /usr/bin/env python

import pkgutil
import logging
logger = logging.getLogger('qless')

# The Lua class
class lua(object):
    def __init__(self, name, r):
        self.name  = name
        self.redis = r
        self.sha   = None
    
    def reload(self):
        data = pkgutil.get_data('qless', 'qless-core/' + self.name + '.lua')
        self.sha = self.redis.execute_command('script', 'load', data)
        logger.debug('Loaded script %s (%s)' % (self.name, repr(self.sha)))
    
    def __call__(self, keys, args):
        if self.sha == None:
            self.reload()
        try:
            return self.redis.execute_command('evalsha', self.sha, len(keys), *(keys + args))
        except Exception as e:
            self.reload()
            return self.redis.execute_command('evalsha', self.sha, len(keys), *(keys + args))
