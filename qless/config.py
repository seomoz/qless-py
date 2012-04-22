#! /usr/bin/env python

import simplejson as json

class Config(object):
    def __init__(self, client):
        self.client = client
    
    def get(self, option=None):
        '''GetConfig(0, [option])
        ----------------------
        Get the current configuration value for that option, or if option is omitted,
        then get all the configuration values.'''
        if option:
            return self.client._getconfig([], [option])
        else:
            return json.loads(self.client._getconfig([], []))
    
    def set(self, option, value=None):
        '''SetConfig(0, option, [value])
        -----------------------------
        Set the configuration value for the provided option. If `value` is omitted,
        then it will remove that configuration option.'''
        if value != None:
            return self.client._setconfig([], [option, value])
        else:
            return self.client._setconfig([], [option])
