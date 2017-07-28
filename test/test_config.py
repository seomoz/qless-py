'''Tests about the config class'''

from common import TestQless


class TestConfig(TestQless):
    '''Test the config class'''
    def test_set_get_unset(self):
        '''Basic set/get/unset'''
        self.assertEqual(self.client.config['foo'], None)
        self.client.config['foo'] = 5
        self.assertEqual(self.client.config['foo'], 5)
        del self.client.config['foo']
        self.assertEqual(self.client.config['foo'], None)

    def test_get_all(self):
        '''Ensure we can get all the configuration'''
        self.assertEqual(self.client.config.all, {
            'application': 'qless',
            'grace-period': 10,
            'heartbeat': 60,
            'histogram-history': 7,
            'jobs-history': 604800,
            'jobs-history-count': 50000,
            'stats-history': 30
        })

    def test_clear(self):
        '''Can unset all keys'''
        original = dict(self.client.config.items())
        for key in self.client.config.keys():
            self.client.config[key] = 1
        for value in self.client.config.values():
            self.assertEqual(value, '1')
        self.client.config.clear()
        self.assertEqual(self.client.config.all, original)

    def test_attribute_error(self):
        '''Only has the 'all' attribute'''
        self.assertRaises(AttributeError, lambda: self.client.config.foo)

    def test_len(self):
        '''We can see how many items are in the config'''
        self.assertEqual(len(self.client.config), 7)

    def test_contains(self):
        '''We can use the 'in' syntax'''
        self.assertFalse('foo' in self.client.config)
        self.client.config['foo'] = 5
        self.assertTrue('foo' in self.client.config)

    def test_iter(self):
        '''We can iterate over the config'''
        self.assertEqual(
            set([key for key in self.client.config]), set(self.client.config.keys()))

    def test_get(self):
        '''We can use dictionary-style get'''
        self.assertFalse('foo' in self.client.config)
        self.assertEqual(self.client.config.get('foo', 5), 5)

    def test_pop(self):
        '''We can use dictionary-style pop'''
        self.assertFalse('foo' in self.client.config)
        self.client.config['foo'] = 5
        self.assertEqual(self.client.config.pop('foo'), 5)
        self.assertFalse('foo' in self.client.config)

    def test_update(self):
        '''We can use dictionary-style update'''
        updated = dict((key, '1') for key in self.client.config)
        self.assertNotEqual(self.client.config.all, updated)
        self.client.config.update(updated)
        self.assertEqual(self.client.config.all, updated)

    def test_default_config(self):
        '''We can get default config values.'''
        self.assertEqual(self.client.config['heartbeat'], 60)
