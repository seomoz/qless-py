'''Basic tests about the Job class'''

from common import TestQless


class TestQueue(TestQless):
    '''Test the Job class'''
    def test_jobs(self):
        '''The queue.Jobs class provides access to job counts'''
        queue = self.client.queues['foo']
        queue.put('Foo', {})
        self.assertEqual(queue.jobs.depends(), [])
        self.assertEqual(queue.jobs.running(), [])
        self.assertEqual(queue.jobs.stalled(), [])
        self.assertEqual(queue.jobs.scheduled(), [])
        self.assertEqual(queue.jobs.recurring(), [])

    def test_counts(self):
        '''Provides access to job counts'''
        self.client.queues['foo'].put('Foo', {})
        self.assertEqual(self.client.queues['foo'].counts, {
            'depends': 0,
            'name': 'foo',
            'paused': False,
            'recurring': 0,
            'running': 0,
            'scheduled': 0,
            'stalled': 0,
            'waiting': 1
        })

    def test_heartbeat(self):
        '''Provided access to heartbeat configuration'''
        original = self.client.queues['foo'].heartbeat
        self.client.queues['foo'].heartbeat = 10
        self.assertNotEqual(original, self.client.queues['foo'].heartbeat)

    def test_attribute_error(self):
        '''Raises an attribute error if there is no attribute'''
        self.assertRaises(AttributeError, lambda: self.client.queues['foo'].foo)

    def test_multipop(self):
        '''Exposes multi-pop'''
        self.client.queues['foo'].put('Foo', {})
        self.client.queues['foo'].put('Foo', {})
        self.assertEqual(len(self.client.queues['foo'].pop(10)), 2)

    def test_peek(self):
        '''Exposes queue peeking'''
        self.client.queues['foo'].put('Foo', {}, jid='jid')
        self.assertEqual(self.client.queues['foo'].peek().jid, 'jid')

    def test_multipeek(self):
        '''Exposes multi-peek'''
        self.client.queues['foo'].put('Foo', {})
        self.client.queues['foo'].put('Foo', {})
        self.assertEqual(len(self.client.queues['foo'].peek(10)), 2)

    def test_stats(self):
        '''Exposes stats'''
        self.client.queues['foo'].stats()

    def test_len(self):
        '''Exposes the length of a queue'''
        self.client.queues['foo'].put('Foo', {})
        self.assertEqual(len(self.client.queues['foo']), 1)
