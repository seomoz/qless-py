'''All our configuration operations'''

import simplejson as json


class Config(object):
    '''A class that allows us to change and manipulate qless config'''
    def __init__(self, client):
        self._client = client

    def __getattr__(self, attr):
        if attr == 'all':
            return json.loads(self._client('config.get'))
        raise AttributeError('qless.Config has no attribute %s' % attr)

    def __len__(self):
        return len(self.all)

    def __getitem__(self, option):
        result = self._client('config.get', option)
        if not result:
            return None
        try:
            return json.loads(result)
        except TypeError:
            return result

    def __setitem__(self, option, value):
        return self._client('config.set', option, value)

    def __delitem__(self, option):
        return self._client('config.unset', option)

    def __contains__(self, option):
        return dict.__contains__(self.all, option)

    def __iter__(self):
        return iter(self.all)

    def clear(self):
        '''Remove all keys'''
        for key in self.all.keys():
            self._client('config.unset', key)

    def get(self, option, default=None):
        '''Get a particular option, or the default if it's missing'''
        val = self[option]
        return (val is None and default) or val

    def items(self):
        '''Just like `dict.items`'''
        return self.all.items()

    def keys(self):
        '''Just like `dict.keys`'''
        return self.all.keys()

    def pop(self, option, default=None):
        '''Just like `dict.pop`'''
        val = self[option]
        del self[option]
        return (val is None and default) or val

    def update(self, other=(), **kwargs):
        '''Just like `dict.update`'''
        _kwargs = dict(kwargs)
        _kwargs.update(other)
        for key, value in _kwargs.items():
            self[key] = value

    def values(self):
        '''Just like `dict.values`'''
        return self.all.values()
