'''Basic tests about the client'''

from common import TestQless


class TestClient(TestQless):
    '''Test the client'''
    def test_track(self):
        '''Gives us access to track and untrack jobs'''
        self.client.queues['foo'].put('Foo', {}, jid='jid')
        self.client.track('jid')
        self.assertEqual(self.client.jobs.tracked()['jobs'][0].jid, 'jid')
        self.client.untrack('jid')
        self.assertEqual(self.client.jobs.tracked(),
            {'jobs': [], 'expired': {}})

    def test_attribute_error(self):
        '''Throws AttributeError for non-attributes'''
        self.assertRaises(AttributeError, lambda: self.client.foo)

    def test_tags(self):
        '''Provides access to top tags'''
        self.assertEqual(self.client.tags(), {})
        for _ in range(10):
            self.client.queues['foo'].put('Foo', {}, tags=['foo'])
        self.assertEqual(self.client.tags(), ['foo'])

    def test_unfail(self):
        '''Provides access to unfail'''
        jids = map(str, range(10))
        for jid in jids:
            self.client.queues['foo'].put('Foo', {}, jid=jid)
            self.client.queues['foo'].pop().fail('foo', 'bar')
        for jid in jids:
            self.assertEqual(self.client.jobs[jid].state, 'failed')
        self.client.unfail('foo', 'foo')
        for jid in jids:
            self.assertEqual(self.client.jobs[jid].state, 'waiting')


class TestJobs(TestQless):
    '''Test the Jobs class'''
    def test_basic(self):
        '''Can give us access to jobs'''
        self.assertEqual(self.client.jobs['jid'], None)
        self.client.queues['foo'].put('Foo', {}, jid='jid')
        self.assertNotEqual(self.client.jobs['jid'], None)

    def test_recurring(self):
        '''Can give us access to recurring jobs'''
        self.assertEqual(self.client.jobs['jid'], None)
        self.client.queues['foo'].recur('Foo', {}, 60, jid='jid')
        self.assertNotEqual(self.client.jobs['jid'], None)

    def test_complete(self):
        '''Can give us access to complete jobs'''
        self.assertEqual(self.client.jobs.complete(), [])
        self.client.queues['foo'].put('Foo', {}, jid='jid')
        self.client.queues['foo'].pop().complete()
        self.assertEqual(self.client.jobs.complete(), ['jid'])

    def test_tracked(self):
        '''Gives us access to tracked jobs'''
        self.assertEqual(self.client.jobs.tracked(),
            {'jobs': [], 'expired': {}})
        self.client.queues['foo'].put('Foo', {}, jid='jid')
        self.client.track('jid')
        self.assertEqual(self.client.jobs.tracked()['jobs'][0].jid, 'jid')

    def test_tagged(self):
        '''Gives us access to tagged jobs'''
        self.assertEqual(self.client.jobs.tagged('foo'),
            {'total': 0, 'jobs': {}})
        self.client.queues['foo'].put('Foo', {}, jid='jid', tags=['foo'])
        self.assertEqual(self.client.jobs.tagged('foo')['jobs'][0], 'jid')

    def test_failed(self):
        '''Gives us access to failed jobs'''
        self.assertEqual(self.client.jobs.failed('foo'),
            {'total': 0, 'jobs': []})
        self.client.queues['foo'].put('Foo', {}, jid='jid')
        self.client.queues['foo'].pop().fail('foo', 'bar')
        self.assertEqual(self.client.jobs.failed('foo')['jobs'][0].jid, 'jid')

    def test_failures(self):
        '''Gives us access to failure types'''
        self.assertEqual(self.client.jobs.failed(), {})
        self.client.queues['foo'].put('Foo', {}, jid='jid')
        self.client.queues['foo'].pop().fail('foo', 'bar')
        self.assertEqual(self.client.jobs.failed(), {'foo': 1})


class TestQueues(TestQless):
    '''Test the Queues class'''
    def test_basic(self):
        '''Gives us access to queues'''
        self.assertNotEqual(self.client.queues['foo'], None)

    def test_counts(self):
        '''Gives us access to counts'''
        self.assertEqual(self.client.queues.counts, {})
        self.client.queues['foo'].put('Foo', {})
        self.assertEqual(self.client.queues.counts, [{
            'scheduled': 0,
            'name': 'foo',
            'paused': False,
            'waiting': 1,
            'depends': 0,
            'running': 0,
            'stalled': 0,
            'recurring': 0
        }])

    def test_attribute_error(self):
        '''Raises AttributeErrors for non-attributes'''
        self.assertRaises(AttributeError, lambda: self.client.queues.foo)


class TestWorkers(TestQless):
    '''Test the Workers class'''
    def test_individual(self):
        '''Gives us access to individual workers'''
        self.client.queues['foo'].put('Foo', {}, jid='jid')
        self.assertEqual(self.client.workers['worker'],
            {'jobs': [], 'stalled': []})
        self.worker.queues['foo'].pop()
        self.assertEqual(self.client.workers['worker'],
            {'jobs': ['jid'], 'stalled': []})

    def test_counts(self):
        '''Gives us access to worker counts'''
        self.client.queues['foo'].put('Foo', {}, jid='jid')
        self.assertEqual(self.client.workers.counts, {})
        self.worker.queues['foo'].pop()
        self.assertEqual(self.client.workers.counts,
            [{'jobs': 1, 'name': 'worker', 'stalled': 0}])

    def test_attribute_error(self):
        '''Raises AttributeErrors for non-attributes'''
        self.assertRaises(AttributeError, lambda: self.client.workers.foo)


# This is used for TestRetry
class Foo(object):
    from qless import retry

    @staticmethod
    @retry(ValueError)
    def process(job):
        '''This is supposed to raise an Exception'''
        if 'valueerror' in job.tags:
            raise ValueError('Foo')
        else:
            raise Exception('Foo')


class TestRetry(TestQless):
    '''Test the retry decorator'''
    def test_basic(self):
        '''Ensure the retry decorator works'''
        # The first time, it should just be retries automatically
        self.client.queues['foo'].put(Foo, {}, tags=['valueerror'], jid='jid')
        self.client.queues['foo'].pop().process()
        # Not remove the tag so it should fail
        self.client.jobs['jid'].untag('valueerror')
        self.client.queues['foo'].pop().process()
        self.assertEqual(self.client.jobs['jid'].state, 'failed')

    def test_docstring(self):
        '''Retry decorator should preserve docstring'''
        self.assertEqual(Foo.process.__doc__,
            'This is supposed to raise an Exception')
