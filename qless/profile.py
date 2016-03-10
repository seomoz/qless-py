'''Some utilities for profiling'''

import redis
from collections import defaultdict


class Profiler(object):
    '''Profiling a series of requests. Initialized with a Qless client'''
    @staticmethod
    def clone(client):
        '''Clone the redis client to be slowlog-compatible'''
        kwargs = client.redis.connection_pool.connection_kwargs
        kwargs['parser_class'] = redis.connection.PythonParser
        pool = redis.connection.ConnectionPool(**kwargs)
        return redis.Redis(connection_pool=pool)

    @staticmethod
    def pretty(timings, label):
        '''Print timing stats'''
        results = [(sum(values), len(values), key)
            for key, values in timings.items()]
        print(label)
        print('=' * 65)
        print('%20s => %13s | %8s | %13s' % (
            'Command', 'Average', '# Calls', 'Total time'))
        print('-' * 65)
        for total, length, key in sorted(results, reverse=True):
            print('%20s => %10.5f us | %8i | %10i us' % (
                key, float(total) / length, length, total))

    def __init__(self, client):
        self._client = self.clone(client)
        self._configs = None
        self._timings = defaultdict(list)
        self._commands = {}

    def start(self):
        '''Get ready for a profiling run'''
        self._configs = self._client.config_get('slow-*')
        self._client.config_set('slowlog-max-len', 100000)
        self._client.config_set('slowlog-log-slower-than', 0)
        self._client.execute_command('slowlog', 'reset')

    def stop(self):
        '''Set everything back to normal and collect our data'''
        for key, value in self._configs.items():
            self._client.config_set(key, value)
        logs = self._client.execute_command('slowlog', 'get', 100000)
        current = {
            'name': None, 'accumulated': defaultdict(list)
        }
        for _, _, duration, request in logs:
            command = request[0]
            if command == 'slowlog':
                continue
            if 'eval' in command.lower():
                subcommand = request[3]
                self._timings['qless-%s' % subcommand].append(duration)
                if current['name']:
                    if current['name'] not in self._commands:
                        self._commands[current['name']] = defaultdict(list)
                    for key, values in current['accumulated'].items():
                        self._commands[current['name']][key].extend(values)
                current = {
                    'name': subcommand, 'accumulated': defaultdict(list)
                }
            else:
                self._timings[command].append(duration)
                if current['name']:
                    current['accumulated'][command].append(duration)
        # Include the last
        if current['name']:
            if current['name'] not in self._commands:
                self._commands[current['name']] = defaultdict(list)
            for key, values in current['accumulated'].items():
                self._commands[current['name']][key].extend(values)

    def display(self):
        '''Print the results of this profiling'''
        self.pretty(self._timings, 'Raw Redis Commands')
        print()
        for key, value in self._commands.items():
            self.pretty(value, 'Qless "%s" Command' % key)
            print()

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, typ, value, trace):
        self.stop()
        self.display()
        if typ:
            raise typ, value, trace
