'''Basic tests about the Job class'''

from common import TestQless
from qless.job import Job, BaseJob


class Foo(object):
    '''A dummy job'''
    @staticmethod
    def bar(job):
        '''A dummy method'''
        job['foo'] = 'bar'
        job.complete()

    def nonstatic(self, job):
        '''A nonstatic method'''
        pass


class TestJob(TestQless):
    '''Test the Job class'''
    def test_attributes(self):
        '''Has all the basic attributes we'd expect'''
        self.client.queues['foo'].put('Foo', {'whiz': 'bang'}, jid='jid',
            tags=['foo'], retries=3)
        job = self.client.jobs['jid']
        atts = ['data', 'jid', 'priority', 'klass_name', 'queue_name', 'tags',
            'expires_at', 'original_retries', 'retries_left', 'worker_name',
            'dependents', 'dependencies']
        values = [getattr(job, att) for att in atts]
        self.assertEqual(dict(zip(atts, values)), {
            'data': {'whiz': 'bang'},
            'dependencies': [],
            'dependents': [],
            'expires_at': 0,
            'jid': 'jid',
            'klass_name': 'Foo',
            'original_retries': 3,
            'priority': 0,
            'queue_name': 'foo',
            'retries_left': 3,
            'tags': ['foo'],
            'worker_name': u''
        })

    def test_set_priority(self):
        '''We can set a job's priority'''
        self.client.queues['foo'].put('Foo', {}, jid='jid', priority=0)
        self.assertEqual(self.client.jobs['jid'].priority, 0)
        self.client.jobs['jid'].priority = 10
        self.assertEqual(self.client.jobs['jid'].priority, 10)

    def test_queue(self):
        '''Exposes a queue object'''
        self.client.queues['foo'].put('Foo', {}, jid='jid')
        self.assertEqual(self.client.jobs['jid'].queue.name, 'foo')

    def test_klass(self):
        '''Exposes the class for a job'''
        self.client.queues['foo'].put(Job, {}, jid='jid')
        self.assertEqual(self.client.jobs['jid'].klass, Job)

    def test_ttl(self):
        '''Exposes the ttl for a job'''
        self.client.config['heartbeat'] = 10
        self.client.queues['foo'].put(Job, {}, jid='jid')
        self.client.queues['foo'].pop()
        self.assertTrue(self.client.jobs['jid'].ttl < 10)
        self.assertTrue(self.client.jobs['jid'].ttl > 9)

    def test_attribute_error(self):
        '''Raises an attribute error for nonexistent attributes'''
        self.client.queues['foo'].put(Job, {}, jid='jid')
        self.assertRaises(AttributeError, lambda: self.client.jobs['jid'].foo)

    def test_cancel(self):
        '''Exposes the cancel method'''
        self.client.queues['foo'].put('Foo', {}, jid='jid')
        self.client.jobs['jid'].cancel()
        self.assertEqual(self.client.jobs['jid'], None)

    def test_tag_untag(self):
        '''Exposes a way to tag and untag a job'''
        self.client.queues['foo'].put('Foo', {}, jid='jid')
        self.client.jobs['jid'].tag('foo')
        self.assertEqual(self.client.jobs['jid'].tags, ['foo'])
        self.client.jobs['jid'].untag('foo')
        self.assertEqual(self.client.jobs['jid'].tags, [])

    def test_getitem(self):
        '''Exposes job data through []'''
        self.client.queues['foo'].put('Foo', {'foo': 'bar'}, jid='jid')
        self.assertEqual(self.client.jobs['jid']['foo'], 'bar')

    def test_setitem(self):
        '''Sets jobs data through []'''
        self.client.queues['foo'].put('Foo', {}, jid='jid')
        job = self.client.jobs['jid']
        job['foo'] = 'bar'
        self.assertEqual(job['foo'], 'bar')

    def test_move(self):
        '''Able to move jobs through the move method'''
        self.client.queues['foo'].put('Foo', {}, jid='jid')
        self.client.jobs['jid'].move('bar')
        self.assertEqual(self.client.jobs['jid'].queue.name, 'bar')

    def test_complete(self):
        '''Able to complete a job'''
        self.client.queues['foo'].put('Foo', {}, jid='jid')
        self.client.queues['foo'].pop().complete()
        self.assertEqual(self.client.jobs['jid'].state, 'complete')

    def test_advance(self):
        '''Able to advance a job to another queue'''
        self.client.queues['foo'].put('Foo', {}, jid='jid')
        self.client.queues['foo'].pop().complete('bar')
        self.assertEqual(self.client.jobs['jid'].state, 'waiting')

    def test_heartbeat(self):
        '''Provides access to heartbeat'''
        self.client.config['heartbeat'] = 10
        self.client.queues['foo'].put('Foo', {}, jid='jid')
        job = self.client.queues['foo'].pop()
        before = job.ttl
        self.client.config['heartbeat'] = 20
        job.heartbeat()
        self.assertTrue(job.ttl > before)

    def test_heartbeat_fail(self):
        '''Failed heartbeats raise an error'''
        from qless.exceptions import LostLockException
        self.client.queues['foo'].put('Foo', {}, jid='jid')
        self.assertRaises(LostLockException, self.client.jobs['jid'].heartbeat)

    def test_track_untrack(self):
        '''Exposes a track, untrack method'''
        self.client.queues['foo'].put('Foo', {}, jid='jid')
        self.assertFalse(self.client.jobs['jid'].tracked)
        self.client.jobs['jid'].track()
        self.assertTrue(self.client.jobs['jid'].tracked)
        self.client.jobs['jid'].untrack()
        self.assertFalse(self.client.jobs['jid'].tracked)

    def test_depend_undepend(self):
        '''Exposes a depend, undepend methods'''
        self.client.queues['foo'].put('Foo', {}, jid='a')
        self.client.queues['foo'].put('Foo', {}, jid='b')
        self.client.queues['foo'].put('Foo', {}, jid='c', depends=['a'])
        self.assertEqual(self.client.jobs['c'].dependencies, ['a'])
        self.client.jobs['c'].depend('b')
        self.assertEqual(self.client.jobs['c'].dependencies, ['a', 'b'])
        self.client.jobs['c'].undepend('a')
        self.assertEqual(self.client.jobs['c'].dependencies, ['b'])
        self.client.jobs['c'].undepend(all=True)
        self.assertEqual(self.client.jobs['c'].dependencies, [])

    def test_retry_fail(self):
        '''Retry raises an error if retry fails'''
        from qless.exceptions import QlessException
        self.client.queues['foo'].put('Foo', {}, jid='jid')
        self.assertRaises(QlessException, self.client.jobs['jid'].retry)

    def test_repr(self):
        '''Has a reasonable repr'''
        self.client.queues['foo'].put(Job, {}, jid='jid')
        self.assertEqual(repr(self.client.jobs['jid']), '<qless.job.Job jid>')

    def test_no_method(self):
        '''Raises an error if the class doesn't have the method'''
        self.client.queues['foo'].put(Foo, {}, jid='jid')
        self.client.queues['foo'].pop().process()
        job = self.client.jobs['jid']
        self.assertEqual(job.state, 'failed')
        self.assertEqual(job.failure['group'], 'foo-method-missing')

    def test_no_import(self):
        '''Raises an error if it can't import the class'''
        self.client.queues['foo'].put('foo.Foo', {}, jid='jid')
        self.client.queues['foo'].pop().process()
        job = self.client.jobs['jid']
        self.assertEqual(job.state, 'failed')
        self.assertEqual(job.failure['group'], 'foo-ImportError')

    def test_nonstatic(self):
        '''Rasises an error if the relevant function's not static'''
        self.client.queues['nonstatic'].put(Foo, {}, jid='jid')
        self.client.queues['nonstatic'].pop().process()
        job = self.client.jobs['jid']
        self.assertEqual(job.state, 'failed')
        self.assertEqual(job.failure['group'], 'nonstatic-method-type')

    def test_reload(self):
        '''Ensure that nothing blows up if we reload a class'''
        self.client.queues['foo'].put(Foo, {}, jid='jid')
        self.assertEqual(self.client.jobs['jid'].klass, Foo)
        Job.reload(self.client.jobs['jid'].klass_name)
        self.assertEqual(self.client.jobs['jid'].klass, Foo)


class TestRecurring(TestQless):
    def test_attributes(self):
        '''We can access all the recurring attributes'''
        self.client.queues['foo'].recur('Foo', {'whiz': 'bang'}, 60, jid='jid',
            tags=['foo'], retries=3)
        job = self.client.jobs['jid']
        atts = ['data', 'jid', 'priority', 'klass_name', 'queue_name', 'tags',
            'retries', 'interval', 'count']
        values = [getattr(job, att) for att in atts]
        self.assertEqual(dict(zip(atts, values)), {
            'count': 0,
            'data': {'whiz': 'bang'},
            'interval': 60,
            'jid': 'jid',
            'klass_name': 'Foo',
            'priority': 0,
            'queue_name': 'foo',
            'retries': 3,
            'tags': ['foo']
        })

    def test_set_priority(self):
        '''We can set priority on recurring jobs'''
        self.client.queues['foo'].recur('Foo', {}, 60, jid='jid', priority=0)
        self.client.jobs['jid'].priority = 10
        self.assertEqual(self.client.jobs['jid'].priority, 10)

    def test_set_retries(self):
        '''We can set retries'''
        self.client.queues['foo'].recur('Foo', {}, 60, jid='jid', retries=2)
        self.client.jobs['jid'].retries = 2
        self.assertEqual(self.client.jobs['jid'].retries, 2)

    def test_set_interval(self):
        '''We can set the interval'''
        self.client.queues['foo'].recur('Foo', {}, 60, jid='jid')
        self.client.jobs['jid'].interval = 10
        self.assertEqual(self.client.jobs['jid'].interval, 10)

    def test_set_data(self):
        '''We can set the job data'''
        self.client.queues['foo'].recur('Foo', {}, 60, jid='jid')
        self.client.jobs['jid'].data = {'foo': 'bar'}
        self.assertEqual(self.client.jobs['jid'].data, {'foo': 'bar'})

    def test_set_klass(self):
        '''We can set the klass'''
        self.client.queues['foo'].recur('Foo', {}, 60, jid='jid')
        self.client.jobs['jid'].klass = Foo
        self.assertEqual(self.client.jobs['jid'].klass, Foo)

    def test_get_next(self):
        '''Exposes the next time a job will run'''
        self.client.queues['foo'].recur('Foo', {}, 60, jid='jid')
        nxt = self.client.jobs['jid'].next
        self.client.queues['foo'].pop()
        self.assertTrue(abs(self.client.jobs['jid'].next - nxt - 60) < 1)

    def test_attribute_error(self):
        '''Raises attribute errors for non-attributes'''
        self.client.queues['foo'].recur('Foo', {}, 60, jid='jid')
        self.assertRaises(AttributeError, lambda: self.client.jobs['jid'].foo)

    def test_move(self):
        '''Exposes a way to move a job'''
        self.client.queues['foo'].recur('Foo', {}, 60, jid='jid')
        self.client.jobs['jid'].move('bar')
        self.assertEqual(self.client.jobs['jid'].queue.name, 'bar')

    def test_cancel(self):
        '''Exposes a way to cancel jobs'''
        self.client.queues['foo'].recur('Foo', {}, 60, jid='jid')
        self.client.jobs['jid'].cancel()
        self.assertEqual(self.client.jobs['jid'], None)

    def test_tag_untag(self):
        '''Exposes a way to tag jobs'''
        self.client.queues['foo'].recur('Foo', {}, 60, jid='jid')
        self.client.jobs['jid'].tag('foo')
        self.assertEqual(self.client.jobs['jid'].tags, ['foo'])
        self.client.jobs['jid'].untag('foo')
        self.assertEqual(self.client.jobs['jid'].tags, [])
