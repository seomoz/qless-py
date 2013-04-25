'''Tests about events'''

from common import TestQless


class TestEvents(TestQless):
    '''Tests about events'''
    def setUp(self):
        TestQless.setUp(self)
        self.client.queues['foo'].put('Foo', {}, jid='jid')
        self.client.jobs['jid'].track()

    def test_basic(self):
        '''Ensure we can get a basic event'''
        def func(_):
            '''No docstring'''
            func.count += 1

        func.count = 0
        self.client.events.on('popped', func)
        self.client.queues['foo'].pop()
        self.client.events.next()
        self.assertEqual(func.count, 1)

    def test_off(self):
        '''Ensure we can turn off callbacks'''
        def popped(_):
            '''No docstring'''
            popped.count += 1

        def completed(_):
            '''No docstring'''
            completed.count += 1

        popped.count = 0
        completed.count = 0
        self.client.events.on('popped', popped)
        self.client.events.on('completed', completed)
        self.client.events.off('popped')
        self.client.queues['foo'].pop().complete()
        self.client.events.next()
        self.client.events.next()
        self.assertEqual(popped.count, 0)
        self.assertEqual(completed.count, 1)

    def test_not_implemented(self):
        '''Ensure missing events throw errors'''
        self.assertRaises(
            NotImplementedError, self.client.events.on, 'foo', int)
