#! /usr/bin/env python

import simplejson as json

class Config(object):
    def __init__(self, client):
        self.client = client
    
    def __getattr__(self, attr):
        '''GetConfig(0, [option])
        ----------------------
        Get the current configuration value for that option, or if option is omitted,
        then get all the configuration values.'''
        if attr == 'all':
            return json.loads(self.client._config([], ['get']))
        raise AttributeError('qless.Config has no attribute %s' % attr)
    
    def __len__(self):
        return len(self.all)
    
    def __getitem__(self, option):
        return self.client._config([], ['get', option])
    
    def __setitem__(self, option, value):
        '''SetConfig(0, option, [value])
        -----------------------------
        Set the configuration value for the provided option. If `value` is omitted,
        then it will remove that configuration option.'''
        return self.client._config([], ['set', option, value])
    
    def __delitem__(self, option):
        return self.client._config([], ['unset', option])
    
    def __contains__(self, option):
        return dict.__contains__(self.all, option)
    
    def __iter__(self):
        return iter(self.all)
    
    def clear(self):
        for key in self.all.keys():
            self.client._config([], ['unset', key])
    
    def get(self, option, default=None):
        r = self[option]
        return ((r == None) and default) or r
    
    def items(self):
        return self.all.items()
    
    def keys(self):
        return self.all.keys()
    
    def pop(self, option, default=None):
        r = self[option]
        del self[option]
        return ((r == None) and default) or r
    
    def update(self, other=(), **kwargs):
        _kwargs = dict(kwargs)
        _kwargs.update(other)
        for key, value in _kwargs.items():
            self[key] = value
    
    def values(self):
        return self.all.values()
