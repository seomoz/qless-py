#! /usr/bin/env python

import time
import math
import qless
import redis
import unittest
import time as _time

class FooJob(qless.Job):
    pass

# Let's mock up time
time._frozen = False

# Save out the original time function
time._time = time.time

def _freeze():
    time._frozen = True
    time._when   = time._time()

def _unfreeze():
    time._frozen = False

def _advance(increment):
    time._when += increment

def _time():
    return (time._frozen and time._when) or time._time()

time.freeze   = _freeze
time.unfreeze = _unfreeze
time.advance  = _advance
time.time     = _time

class TestQless(unittest.TestCase):
    def setUp(self):
        self.redis  = redis.Redis()
        
        assert(len(self.redis.keys('*')) == 0)
        
        # Clear the script cache, and nuke everything
        self.redis.execute_command('script', 'flush')
        
        # The qless client we're using
        self.client = qless.client()
        # Our main queue
        self.q = self.client.queues['testing']
        
        # This represents worker 'a'
        tmp = qless.client(); tmp.worker_name = 'worker-a'
        self.a = tmp.queues['testing']
        # This represents worker b
        tmp = qless.client(); tmp.worker_name = 'worker-b'
        self.b = tmp.queues['testing']
                
        # This is just a second queue
        self.other = self.client.queues['other']
    
    def tearDown(self):
        self.redis.flushdb()
        time.unfreeze()

class TestRecurring(TestQless):
    def test_recur_on(self):
        # In this test, we want to enqueue a job and make sure that
        # we can get some jobs from it in the most basic way. We should
        # get jobs out of the queue every _k_ seconds
        time.freeze()
        self.q.recur(qless.Job, {'test':'test_recur_on'}, interval=1800)
        self.assertEqual(self.q.pop(), None)
        time.advance(1799)
        self.assertEqual(self.q.pop(), None)
        time.advance(2)
        job = self.q.pop()
        self.assertNotEqual(job, None)
        self.assertEqual(job.data, {'test':'test_recur_on'})
        job.complete()
        # We should not be able to pop a second job
        self.assertEqual(self.q.pop(), None)
        # Let's advance almost to the next one, and then check again
        time.advance(1798)
        self.assertEqual(self.q.pop(), None)
        time.advance(2)
        self.assertNotEqual(self.q.pop(), None)
        time.unfreeze()
    
    def test_recur_attributes(self):
        # Popped jobs should have the same priority, tags, etc. that the
        # recurring job has
        time.freeze()
        self.q.recur(qless.Job, {'test':'test_recur_attributes'}, interval=100, priority=-10, tags=['foo', 'bar'], retries=2)
        for i in range(10):
            time.advance(100)
            job = self.q.pop()
            self.assertNotEqual(job, None)
            self.assertEqual(job.priority, -10)
            self.assertEqual(job.tags, ['foo', 'bar'])
            self.assertEqual(job.original_retries, 2)
            self.assertIn(   job.jid, self.client.jobs.tagged('foo')['jobs'])
            self.assertIn(   job.jid, self.client.jobs.tagged('bar')['jobs'])
            self.assertNotIn(job.jid, self.client.jobs.tagged('hey')['jobs'])
            job.complete()
            self.assertEqual(self.q.pop(), None)
        time.unfreeze()
    
    def test_recur_offset(self):
        # In this test, we should get a job after offset and interval
        # have passed
        time.freeze()
        self.q.recur(qless.Job, {'test':'test_recur_offset'}, interval=100, offset=50)
        self.assertEqual(self.q.pop(), None)
        time.advance(100)
        self.assertEqual(self.q.pop(), None)
        time.advance(50)
        job = self.q.pop()
        self.assertNotEqual(job, None)
        job.complete()
        # And henceforth we should have jobs periodically at 100 seconds
        time.advance(99)
        self.assertEqual(self.q.pop(), None)
        time.advance(2)
        self.assertNotEqual(self.q.pop(), None)
    
    def test_recur_off(self):
        # In this test, we want to make sure that we can stop recurring
        # jobs
        # We should see these recurring jobs crop up under queues when 
        # we request them
        time.freeze()
        jid = self.q.recur(qless.Job, {'test':'test_recur_off'}, interval=100)
        self.assertEqual(self.client.queues['testing'].counts['recurring'], 1)
        self.assertEqual(self.client.queues.counts[0]['recurring'], 1)
        # Now, let's pop off a job, and then cancel the thing
        time.advance(110)
        self.assertEqual(self.q.pop().complete(), 'complete')
        job = self.client.jobs[jid]
        self.assertEqual(job.__class__, qless.RecurringJob)
        job.cancel()
        self.assertEqual(self.client.queues['testing'].counts['recurring'], 0)
        self.assertEqual(self.client.queues.counts[0]['recurring'], 0)
        time.advance(1000)
        self.assertEqual(self.q.pop(), None)
    
    def test_jobs_recur(self):
        # We should be able to list the jids of all the recurring jobs
        # in a queue
        jids = [self.q.recur(qless.Job, {'test':'test_jobs_recur'}, interval=i * 10) for i in range(10)]
        self.assertEqual(self.q.jobs.recurring(), jids)
        for jid in jids:
            self.assertEqual(self.client.jobs[jid].__class__, qless.RecurringJob)
    
    def test_recur_get(self):
        # We should be able to get the data for a recurring job
        time.freeze()
        jid = self.q.recur(qless.Job, {'test':'test_recur_get'}, interval=100, priority=-10, tags=['foo', 'bar'], retries=2)
        job = self.client.jobs[jid]
        self.assertEqual(job.__class__ , qless.RecurringJob)
        self.assertEqual(job.priority  , -10)
        self.assertEqual(job.queue_name, 'testing')
        self.assertEqual(job.data      , {'test':'test_recur_get'})
        self.assertEqual(job.tags      , ['foo', 'bar'])
        self.assertEqual(job.interval  , 100)
        self.assertEqual(job.retries   , 2)
        self.assertEqual(job.count     , 0)
        self.assertEqual(job.klass_name, 'qless.job.Job')
        
        # Now let's pop a job
        time.advance(110)
        self.q.pop()
        self.assertEqual(self.client.jobs[jid].count, 1)
    
    def test_passed_interval(self):
        # We should get multiple jobs if we've passed the interval time
        # several times.
        time.freeze()
        jid = self.q.recur(qless.Job, {'test':'test_passed_interval'}, interval=100)
        time.advance(850)
        self.assertEqual(len(self.q.pop(100)), 8)
    
    def test_queues_endpoint(self):
        # We should see these recurring jobs crop up under queues when 
        # we request them
        jid = self.q.recur(qless.Job, {'test':'test_queues_endpoint'}, interval=100)
        self.assertEqual(self.client.queues['testing'].counts['recurring'], 1)
        self.assertEqual(self.client.queues.counts[0]['recurring'], 1)
    
    def test_change_attributes(self):
        # We should be able to change the attributes of a recurring job,
        # and future spawned jobs should be affected appropriately. In
        # addition, when we change the interval, the effect should be 
        # immediate (evaluated from the last time it was run)
        time.freeze()
        jid = self.q.recur(qless.Job, {'test':'test_change_attributes'}, interval=1)
        job = self.client.jobs[jid]
        
        # First, test priority
        time.advance(1)
        self.assertNotEqual(self.q.pop().priority         , -10)
        self.assertNotEqual(self.client.jobs[jid].priority, -10)
        job.priority = -10
        time.advance(1)
        self.assertEqual(   self.q.pop().priority         , -10)
        self.assertEqual(   self.client.jobs[jid].priority, -10)
        
        # And data
        time.advance(1)
        self.assertNotEqual(self.q.pop().data             , {'foo': 'bar'})
        self.assertNotEqual(self.client.jobs[jid].data    , {'foo': 'bar'})
        job.data     = {'foo': 'bar'}
        time.advance(1)
        self.assertEqual(   self.q.pop().data             , {'foo': 'bar'})
        self.assertEqual(   self.client.jobs[jid].data    , {'foo': 'bar'})
        
        # And retries
        time.advance(1)
        self.assertNotEqual(self.q.pop().original_retries , 10)
        self.assertNotEqual(self.client.jobs[jid].retries , 10)
        job.retries  = 10
        time.advance(1)
        self.assertEqual(   self.q.pop().original_retries , 10)
        self.assertEqual(   self.client.jobs[jid].retries , 10)
        
        # And klass
        time.advance(1)
        self.assertNotEqual(self.q.peek().klass_name        , 'qless.job.RecurringJob')
        self.assertNotEqual(self.q.pop().klass              , qless.RecurringJob)
        self.assertNotEqual(self.client.jobs[jid].klass_name, 'qless.job.RecurringJob')
        self.assertNotEqual(self.client.jobs[jid].klass     , qless.RecurringJob)
        job.klass    = qless.RecurringJob
        time.advance(1)
        self.assertEqual(   self.q.peek().klass_name        , 'qless.job.RecurringJob')
        self.assertEqual(   self.q.pop().klass              , qless.RecurringJob)
        self.assertEqual(   self.client.jobs[jid].klass_name, 'qless.job.RecurringJob')
        self.assertEqual(   self.client.jobs[jid].klass     , qless.RecurringJob)
    
    def test_change_interval(self):
        # If we update a recurring job's interval, then we should get
        # jobs from it as if it had been scheduled this way from the
        # last time it had a job popped
        time.freeze()
        jid = self.q.recur(qless.Job, {'test':'test_change_interval'}, interval=100)
        time.advance(100)
        self.assertEqual(self.q.pop().complete(), 'complete')
        time.advance(50)
        # Now, let's update the interval to make it more frequent
        self.client.jobs[jid].interval = 10
        jobs = self.q.pop(100)
        self.assertEqual(len(jobs), 5)
        results = [job.complete() for job in jobs]
        # Now let's make the interval much longer
        time.advance(49) ; self.client.jobs[jid].interval = 1000; self.assertEqual(self.q.pop(), None)
        time.advance(100); self.client.jobs[jid].interval = 1000; self.assertEqual(self.q.pop(), None)
        time.advance(849); self.client.jobs[jid].interval = 1000; self.assertEqual(self.q.pop(), None)
        time.advance(1)  ; self.client.jobs[jid].interval = 1000; self.assertEqual(self.q.pop(), None)
    
    def test_move(self):
        # If we move a recurring job from one queue to another, then
        # all future spawned jobs should be popped from that queue
        time.freeze()
        jid = self.q.recur(qless.Job, {'test':'test_move'}, interval = 100)
        time.advance(110)
        self.assertEqual(self.q.pop().complete(), 'complete')
        self.assertEqual(self.other.pop(), None)
        # Now let's move it to another queue
        self.client.jobs[jid].move('other')
        self.assertEqual(self.q.pop()    , None)
        self.assertEqual(self.other.pop(), None)
        time.advance(100)
        self.assertEqual(self.q.pop()    , None)
        self.assertEqual(self.other.pop().complete(), 'complete')
    
    def test_change_tags(self):
        # We should be able to add and remove tags from a recurring job,
        # and see the impact in all the jobs it subsequently spawns
        time.freeze()
        jid = self.q.recur(qless.Job, {'test':'test_change_tags'}, tags = ['foo', 'bar'], interval = 1)
        time.advance(1)
        self.assertEqual(self.q.pop().tags, ['foo', 'bar'])
        # Now let's untag the job
        self.client.jobs[jid].untag('foo')
        self.assertEqual(self.client.jobs[jid].tags, ['bar'])
        time.advance(1)
        self.assertEqual(self.q.pop().tags, ['bar'])
        
        # Now let's add 'foo' back in, and also add 'hey'
        self.client.jobs[jid].tag('foo', 'hey')
        self.assertEqual(self.client.jobs[jid].tags, ['bar', 'foo', 'hey'])
        time.advance(1)
        self.assertEqual(self.q.pop().tags, ['bar', 'foo', 'hey'])
    
    def test_peek(self):
        # When we peek at jobs in a queue, it should take recurring jobs
        # into account
        time.freeze()
        jid = self.q.recur(qless.Job, {'test':'test_peek'}, interval = 100)
        self.assertEqual(self.q.peek(), None)
        time.advance(110)
        self.assertNotEqual(self.q.peek(), None)

class TestDependencies(TestQless):
    def test_depends_put(self):
        # In this test, we want to put a job, and put a second job
        # that depends on it. We'd then like to verify that it's 
        # only available for popping once its dependency has completed
        jid = self.q.put(qless.Job, {'test': 'depends_put'})
        job = self.q.pop()
        jid = self.q.put(qless.Job, {'test': 'depends_put'}, depends=[job.jid])
        self.assertEqual(self.q.pop(), None)
        self.assertEqual(self.client.jobs[jid].state, 'depends')
        job.complete()
        self.assertEqual(self.client.jobs[jid].state, 'waiting')
        self.assertEqual(self.q.pop().jid, jid)
        
        # Let's try this dance again, but with more job dependencies
        jids = [self.q.put(qless.Job, {'test': 'depends_put'}) for i in range(10)]
        jid  = self.q.put(qless.Job, {'test': 'depends_put'}, depends=jids)
        # Pop more than we put on
        jobs = self.q.pop(20)
        self.assertEqual(len(jobs), 10)
        # Complete them, and then make sure the last one's available
        for job in jobs:
            self.assertEqual(self.q.pop(), None)
            job.complete()
        
        # It's only when all the dependencies have been completed that
        # we should be able to pop this job off
        self.assertEqual(self.q.pop().jid, jid)
    
    def test_depends_complete(self):
        # In this test, we want to put a job, put a second job, and
        # complete the first job, making it dependent on the second
        # job. This should test the ability to add dependency during
        # completion
        a = self.q.put(qless.Job, {'test': 'depends_complete'})
        b = self.q.put(qless.Job, {'test': 'depends_complete'})
        job = self.q.pop()
        job.complete('testing', depends=[b])
        self.assertEqual(self.client.jobs[a].state, 'depends')
        jobs = self.q.pop(20)
        self.assertEqual(len(jobs), 1)
        jobs[0].complete()
        self.assertEqual(self.client.jobs[a].state, 'waiting')
        job = self.q.pop()
        self.assertEqual(job.jid, a)
        
        # Like above, let's try this dance again with more dependencies
        jids = [self.q.put(qless.Job, {'test': 'depends_put'}) for i in range(10)]
        jid  = job.jid
        job.complete('testing', depends=jids)
        # Pop more than we put on
        jobs = self.q.pop(20)
        self.assertEqual(len(jobs), 10)
        # Complete them, and then make sure the last one's available
        for job in jobs:
            j = self.q.pop()
            self.assertEqual(j, None)
            job.complete()
        
        # It's only when all the dependencies have been completed that
        # we should be able to pop this job off
        self.assertEqual(self.q.pop().jid, jid)
    
    def test_depends_state(self):
        # Put a job, and make it dependent on a canceled job, and a
        # non-existent job, and a complete job. It should be available
        # from the start.
        jids = ['foobar', 
            self.q.put(qless.Job, {'test': 'test_depends_state'}),
            self.q.put(qless.Job, {'test': 'test_depends_state'})]
        
        # Cancel one, complete one
        self.q.pop().cancel()
        self.q.pop().complete()
        # Ensure there are none in the queue, then put one, should pop right off
        self.assertEqual(len(self.q), 0)
        jid = self.q.put(qless.Job, {'test': 'test_depends_state'}, depends=jids)
        self.assertEqual(self.q.pop().jid, jid)
    
    def test_depends_canceled(self):
        # B is dependent on A, but then we cancel B, then A is still
        # able to complete without any problems. If you try to cancel
        # a job that others depend on, you should have an exception thrown
        a = self.q.put(qless.Job, {'test': 'test_depends_canceled'})
        b = self.q.put(qless.Job, {'test': 'test_depends_canceled'}, depends=[a])
        self.client.jobs[b].cancel()
        job = self.q.pop()
        self.assertEqual(job.jid, a)
        self.assertEqual(job.complete(), 'complete')
        self.assertEqual(self.q.pop(), None)
        
        a = self.q.put(qless.Job, {'test': 'cancel_dependency'})
        b = self.q.put(qless.Job, {'test': 'cancel_dependency'}, depends=[a])
        try:
            self.assertTrue(self.client.jobs[a].cancel(), 'We should not be able to cancel jobs with dependencies')
        except Exception as e:
            self.assertTrue('Cancel()' in e.message, 'Cancel() threw the wrong error')
        
        # When canceling a job, we should remove that job from the jobs' list
        # of dependents.
        self.client.jobs[b].cancel()
        self.assertEqual(self.client.jobs[a].dependents, [])
        # We should also just be able to cancel a now
        self.client.jobs[a].cancel()
    
    def test_depends_complete_advance(self):
        # If we make B depend on A, and then move A through several
        # queues, then B should only be availble once A has finished
        # its whole run.
        a = self.q.put(qless.Job, {'test': 'test_depends_advance'})
        b = self.q.put(qless.Job, {'test': 'test_depends_advance'}, depends=[a])
        for i in range(10):
            job = self.q.pop()
            self.assertEqual(job.jid, a)
            job.complete('testing')
        
        self.q.pop().complete()
        self.assertEqual(self.q.pop().jid, b)
    
    def test_cascading_dependency(self):
        # If we make a dependency chain, then we validate that we can
        # only access them one at a time, in the order of their dependency
        jids = [self.q.put(qless.Job, {'test': 'cascading_depencency'})]
        for i in range(10):
            jids.append(self.q.put(qless.Job, {'test': 'cascading_dependency'}, depends=[jids[-1]]))
        
        # Pop off the first 10 dependencies, ensuring only one comes off at a time
        for i in range(11):
            jobs = self.q.pop(10)
            self.assertEqual(len(jobs), 1)
            self.assertEqual(jobs[0].jid, jids[i])
            jobs[0].complete()
    
    def test_move_dependency(self):
        # If we put a job into a queue with dependencies, and then 
        # move it to another queue, then all the original dependencies
        # should be honored. The reason for this is that dependencies
        # can always be removed after the fact, but this prevents us
        # from the running the risk of moving a job, and it getting 
        # popped before we can describe its dependencies
        a = self.q.put(qless.Job, {'test': 'move_dependency'})
        b = self.q.put(qless.Job, {'test': 'move_dependency'}, depends=[a])
        self.client.jobs[b].move('other')
        self.assertEqual(self.client.jobs[b].state, 'depends')
        self.assertEqual(self.other.pop(), None)
        self.q.pop().complete()
        self.assertEqual(self.client.jobs[b].state, 'waiting')
        self.assertEqual(self.other.pop().jid, b)
    
    def test_add_dependency(self):
        # If we have a job that already depends on on other jobs, then
        # we should be able to add more dependencies. If it's not, then
        # we can't
        a = self.q.put(qless.Job, {'test': 'add_dependency'})
        b = self.client.jobs[self.q.put(qless.Job, {'test': 'add_dependency'}, depends=[a])]
        c = self.q.put(qless.Job, {'test': 'add_dependency'})
        self.assertEqual(b.depend(c), True)
        
        jobs = self.q.pop(20)
        self.assertEqual(len(jobs), 2)
        self.assertEqual(jobs[0].jid, a)
        self.assertEqual(jobs[1].jid, c)
        jobs[0].complete(); jobs[1].complete()
        job = self.q.pop()
        self.assertEqual(job.jid, b.jid)
        job.complete()
        
        # If the job's put, but waiting, we can't add dependencies
        a = self.q.put(qless.Job, {'test': 'add_dependency'})
        b = self.q.put(qless.Job, {'test': 'add_dependency'})
        self.assertEqual(self.client.jobs[a].depend(b), False)
        job = self.q.pop()
        self.assertEqual(job.depend(b), False)
        job.fail('what', 'something')
        self.assertEqual(self.client.jobs[job.jid].depend(b), False)
    
    def test_remove_dependency(self):
        # If we have a job that already depends on others, then we should
        # we able to remove them. If it's not dependent on any, then we can't.
        a = self.q.put(qless.Job, {'test': 'remove_dependency'})
        b = self.client.jobs[self.q.put(qless.Job, {'test': 'remove_dependency'}, depends=[a])]
        self.assertEqual(len(self.q.pop(20)), 1)
        b.undepend(a)
        self.assertEqual(self.q.pop().jid, b.jid)
        # Make sure we removed the dependents from the first one, as well
        self.assertEqual(self.client.jobs[a].dependents, [])
        
        # Let's try removing /all/ dependencies
        jids = [self.q.put(qless.Job, {'test': 'remove_dependency'}) for i in range(10)]
        b = self.client.jobs[self.q.put(qless.Job, {'test': 'remove_dependency'}, depends=jids)]
        self.assertEqual(len(self.q.pop(20)), 10)
        b.undepend(all=True)
        self.assertEqual(self.client.jobs[b.jid].state, 'waiting')
        # Let's make sure that each of the jobs we removed as dependencies also go their
        # dependencies removed, too.
        for jid in jids:
            self.assertEqual(self.client.jobs[jid].dependents, [])
        
        # If the job's put, but waiting, we can't add dependencies
        a = self.q.put(qless.Job, {'test': 'add_dependency'})
        b = self.q.put(qless.Job, {'test': 'add_dependency'})
        self.assertEqual(self.client.jobs[a].undepend(b), False)
        job = self.q.pop()
        self.assertEqual(job.undepend(b), False)
        job.fail('what', 'something')
        self.assertEqual(self.client.jobs[job.jid].undepend(b), False)
    
    def test_jobs_depends(self):
        # When we have jobs that have dependencies, we should be able to
        # get access to them.
        a = self.q.put(qless.Job, {'test': 'jobs_depends'})
        b = self.q.put(qless.Job, {'test': 'jobs_depends'}, depends=[a])
        self.assertEqual(self.client.queues.counts[0]['depends'], 1)
        self.assertEqual(self.client.queues['testing'].counts['depends'], 1)
        self.assertEqual(self.q.jobs.depends(), [b])
        
        # When we remove a dependency, we should no longer see that job as a dependency
        self.client.jobs[b].undepend(a)
        self.assertEqual(self.client.queues.counts[0]['depends'], 0)
        self.assertEqual(self.client.queues['testing'].counts['depends'], 0)
        self.assertEqual(self.q.jobs.depends(), [])
        
        # When we move a job that has a dependency, we should no longer
        # see it in the depends() of the original job
        a = self.q.put(qless.Job, {'test': 'jobs_depends'})
        b = self.q.put(qless.Job, {'test': 'jobs_depends'}, depends=[a])
        self.assertEqual(self.client.queues.counts[0]['depends'], 1)
        self.assertEqual(self.client.queues['testing'].counts['depends'], 1)
        self.assertEqual(self.q.jobs.depends(), [b])
        # Now, move the job
        self.client.jobs[b].move('other')
        self.assertEqual(self.client.queues.counts[0]['depends'], 0)
        self.assertEqual(self.client.queues['testing'].counts['depends'], 0)
        self.assertEqual(self.q.jobs.depends(), [])

class TestRetry(TestQless):
    # It should decrement retries, and put it back in the queue. If retries
    # have been exhausted, then it should be marked as failed.
    # Prohibitions:
    #   1) We can't retry from another worker
    #   2) We can't retry if it's not running
    def test_retry(self):
        jid = self.q.put(qless.Job, {'test': 'test_retry'})
        job = self.q.pop()
        self.assertEqual(job.original_retries, job.retries_left)
        job.retry()
        # Pop it off again
        self.assertEqual(self.q.jobs.scheduled(), [])
        self.assertEqual(self.client.jobs[job.jid].state, 'waiting')
        job = self.q.pop()
        self.assertNotEqual(job, None)
        self.assertEqual(job.original_retries, job.retries_left + 1)
        # Retry it again, with a backoff
        job.retry(60)
        self.assertEqual(self.q.pop(), None)
        self.assertEqual(self.q.jobs.scheduled(), [jid])
        job = self.client.jobs[jid]
        self.assertEqual(job.original_retries, job.retries_left + 2)
        self.assertEqual(job.state, 'scheduled')
    
    def test_retry_fail(self):
        # Make sure that if we exhaust a job's retries, that it fails
        jid = self.q.put(qless.Job, {'test': 'test_retry_fail'}, retries=2)
        self.assertEqual(self.client.jobs.failed(), {})
        self.assertEqual(self.q.pop().retry(), 1)
        self.assertEqual(self.q.pop().retry(), 0)
        self.assertEqual(self.q.pop().retry(), -1)
        self.assertEqual(self.client.jobs.failed(), {
            'failed-retries-testing': 1
        })
    
    def test_retry_error(self):
        # These are some of the conditions under which we cannot retry a job
        job = self.client.jobs[self.q.put(qless.Job, {'test': 'test_retry_error'})]
        self.assertEqual(job.retry(), False)
        self.q.pop().fail('foo', 'bar')
        self.assertEqual(self.client.jobs[job.jid].retry(), False)
        self.client.jobs[job.jid].move('testing')
        job = self.q.pop(); job.worker_name = 'foobar'
        self.assertEqual(job.retry(), False)
        job.worker_name = self.client.worker_name
        job.complete()
        self.assertEqual(job.retry(), False)
    
    def test_retry_workers(self):
        # When we retry a job, it shouldn't be reported as belonging to that worker
        # any longer
        jid = self.q.put(qless.Job, {'test': 'test_retry_workers'})
        job = self.q.pop()
        self.assertEqual(self.client.workers[self.client.worker_name], {'jobs': [jid], 'stalled': []})
        self.assertEqual(job.retry(), 4)
        self.assertEqual(self.client.workers[self.client.worker_name], {'jobs': [], 'stalled': []})

class TestPriority(TestQless):
    # Basically all we need to test:
    # 1) If the job doesn't exist, then attempts to set the priority should
    #   return false. This doesn't really matter for us since we're using the
    #   __setattr__ magic method
    # 2) If the job's in a queue, but not yet popped, we should update its
    #   priority in that queue.
    # 3) If a job's in a queue, but already popped, then we just update the 
    #   job's priority.
    def test_priority(self):
        a = self.q.put(qless.Job, {'test': 'test_priority'}, priority = -10)
        b = self.q.put(qless.Job, {'test': 'test_priority'})
        self.assertEqual(self.q.peek().jid, a)
        job = self.client.jobs[b]
        job.priority = -20
        self.assertEqual(len(self.q), 2)
        self.assertEqual(self.q.peek().jid, b)
        job = self.q.pop()
        self.assertEqual(len(self.q), 2)
        self.assertEqual(job.jid, b)
        job = self.q.pop()
        self.assertEqual(len(self.q), 2)
        self.assertEqual(job.jid, a)
        job.priority = -30
        # Make sure it didn't get doubly-inserted in the queue
        self.assertEqual(len(self.q), 2)
        self.assertEqual(self.q.peek(), None)
        self.assertEqual(self.q.pop(), None)

class TestTag(TestQless):
    # 1) Should make sure that when we double-tag an item, that we don't
    #   see it show up twice when we get it back with the job
    # 2) Should also preserve tags in the order in which they were inserted
    # 3) When a job expires or is canceled, it should be removed from the 
    #   set of jobs with that tag
    def test_tag(self):
        job = self.client.jobs[self.q.put(qless.Job, {'test': 'tag'})]
        self.assertEqual(self.client.jobs.tagged('foo'), {'total': 0, 'jobs': {}})
        self.assertEqual(self.client.jobs.tagged('bar'), {'total': 0, 'jobs': {}})
        job.tag('foo')
        self.assertEqual(self.client.jobs.tagged('foo'), {'total': 1, 'jobs': [job.jid]})
        self.assertEqual(self.client.jobs.tagged('bar'), {'total': 0, 'jobs': {}})
        job.tag('bar')
        self.assertEqual(self.client.jobs.tagged('foo'), {'total': 1, 'jobs': [job.jid]})
        self.assertEqual(self.client.jobs.tagged('bar'), {'total': 1, 'jobs': [job.jid]})
        job.untag('foo')
        self.assertEqual(self.client.jobs.tagged('foo'), {'total': 0, 'jobs': {}})
        self.assertEqual(self.client.jobs.tagged('bar'), {'total': 1, 'jobs': [job.jid]})
        job.untag('bar')
        self.assertEqual(self.client.jobs.tagged('foo'), {'total': 0, 'jobs': {}})
        self.assertEqual(self.client.jobs.tagged('bar'), {'total': 0, 'jobs': {}})
    
    def test_preserve_order(self):
        job = self.client.jobs[self.q.put(qless.Job, {'test': 'preserve_order'})]
        tags = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h']
        for i in range(len(tags)):
            job.tag(tags[i])
            self.assertEqual(self.client.jobs[job.jid].tags, tags[0:i+1])
        
        # Now let's take a select few out
        job.untag('a', 'c', 'e', 'g')
        self.assertEqual(self.client.jobs[job.jid].tags, ['b', 'd', 'f', 'h'])
    
    def test_cancel_expire(self):
        # First, we'll cancel a job
        job = self.client.jobs[self.q.put(qless.Job, {'test': 'cancel_expire'})]
        job.tag('foo', 'bar')
        self.assertEqual(self.client.jobs.tagged('foo'), {'total': 1, 'jobs': [job.jid]})
        self.assertEqual(self.client.jobs.tagged('bar'), {'total': 1, 'jobs': [job.jid]})
        job.cancel()
        self.assertEqual(self.client.jobs.tagged('foo'), {'total': 0, 'jobs': {}})
        self.assertEqual(self.client.jobs.tagged('bar'), {'total': 0, 'jobs': {}})
        
        # Now, we'll have a job expire from completion
        self.client.config.set('jobs-history-count', 0)
        self.q.put(qless.Job, {'test': 'cancel_expire'})
        job = self.q.pop()
        self.assertNotEqual(job, None)
        job.tag('foo', 'bar')
        self.assertEqual(self.client.jobs.tagged('foo'), {'total': 1, 'jobs': [job.jid]})
        self.assertEqual(self.client.jobs.tagged('bar'), {'total': 1, 'jobs': [job.jid]})
        job.complete()
        self.assertEqual(self.client.jobs[job.jid], None)
        self.assertEqual(self.client.jobs.tagged('foo'), {'total': 0, 'jobs': {}})
        self.assertEqual(self.client.jobs.tagged('bar'), {'total': 0, 'jobs': {}})
        
        # If the job no longer exists, attempts to tag it should not add to the set
        job.tag('foo', 'bar')
        self.assertEqual(self.client.jobs.tagged('foo'), {'total': 0, 'jobs': {}})
        self.assertEqual(self.client.jobs.tagged('bar'), {'total': 0, 'jobs': {}})
    
    def test_tag_put(self):
        # We should make sure that we can tag a job when we initially put it, too
        self.assertEqual(self.client.jobs.tagged('foo'), {'total': 0, 'jobs': {}})
        self.assertEqual(self.client.jobs.tagged('bar'), {'total': 0, 'jobs': {}})
        jid = self.q.put(qless.Job, {'test': 'tag_put'}, tags=['foo', 'bar'])
        self.assertEqual(self.client.jobs.tagged('foo'), {'total': 1, 'jobs': [jid]})
        self.assertEqual(self.client.jobs.tagged('bar'), {'total': 1, 'jobs': [jid]})
    
    def test_tag_top(self):
        # 1) Make sure that it only includes tags with more than one job associated with it
        # 2) Make sure that when jobs are untagged, it decrements the count
        # 3) When we tag a job, it increments the count
        # 4) When jobs complete and expire, it decrements the count
        # 5) When jobs are put, make sure it shows up in the tags
        # 6) When canceled, decrements
        self.assertEqual(self.client.tags(), {})
        jids = [self.q.put(qless.Job, {}, tags=['foo']) for i in range(10)]
        self.assertEqual(self.client.tags(), ['foo'])
        jobs = [self.client.jobs[jid].cancel() for jid in jids]
        self.assertEqual(self.client.tags(), {})
        # Add only one back
        a = self.q.put(qless.Job, {}, tags=['foo'])
        self.assertEqual(self.client.tags(), {})
        # Add a second, and then tag it
        b = self.client.jobs[self.q.put(qless.Job, {})]
        b.tag('foo')
        self.assertEqual(self.client.tags(), ['foo'])
        b.untag('foo')
        self.assertEqual(self.client.tags(), {})
        b.tag('foo')
        # Test job expiration
        self.client.config.set('jobs-history-count', 0)
        self.assertEqual(len(self.q), 2)
        self.q.pop().complete()
        self.assertEqual(self.client.tags(), {})

class TestFail(TestQless):
    def test_fail_failed(self):
        # In this test, we want to make sure that we can correctly 
        # fail a job
        #   1) Put a job
        #   2) Fail a job
        #   3) Ensure the queue is empty, and that there's something
        #       in the failed endpoint
        #   4) Ensure that the job still has its original queue
        self.assertEqual(len(self.q), 0, 'Start with an empty queue')
        self.assertEqual(len(self.client.jobs.failed()), 0)
        jid = self.q.put(qless.Job, {'test': 'fail_failed'})
        job = self.q.pop()
        job.fail('foo', 'Some sort of message')
        self.assertEqual(self.q.pop(), None)
        self.assertEqual(self.client.jobs.failed(), {
            'foo': 1
        })
        results = self.client.jobs.failed('foo')
        self.assertEqual(results['total'], 1)
        job = results['jobs'][0]
        self.assertEqual(job.jid        , jid)
        self.assertEqual(job.queue_name , 'testing')
        self.assertEqual(job.queue.name , 'testing')
        self.assertEqual(job.data       , {'test': 'fail_failed'})
        self.assertEqual(job.worker_name, '')
        self.assertEqual(job.state      , 'failed')
        self.assertEqual(job.retries_left , 5)
        self.assertEqual(job.original_retries   , 5)
        self.assertEqual(job.klass_name , 'qless.job.Job')
        self.assertEqual(job.klass      , qless.Job)
        self.assertEqual(job.tags       , [])
    
    def test_pop_fail(self):
        # In this test, we want to make sure that we can pop a job,
        # fail it, and then we shouldn't be able to complete /or/ 
        # heartbeat the job
        #   1) Put a job
        #   2) Fail a job
        #   3) Heartbeat to job fails
        #   4) Complete job fails
        self.assertEqual(len(self.q), 0, 'Start with an empty queue')
        self.assertEqual(len(self.client.jobs.failed()), 0)
        jid = self.q.put(qless.Job, {'test': 'pop_fail'})
        job = self.q.pop()
        self.assertNotEqual(job, None)
        job.fail('foo', 'Some sort of message')
        self.assertEqual(len(self.q), 0)
        self.assertEqual(job.heartbeat(), False)
        self.assertEqual(job.complete() , False)
        self.assertEqual(self.client.jobs.failed(), {
            'foo': 1
        })
        results = self.client.jobs.failed('foo')
        self.assertEqual(results['total'], 1)
        self.assertEqual(results['jobs'][0].jid, jid)
    
    def test_fail_state(self):
        # We shouldn't be able to fail a job that's in any state but
        # running
        self.assertEqual(len(self.client.jobs.failed()), 0)
        job = self.client.jobs[self.q.put(qless.Job, {'test': 'fail_state'})]
        self.assertEqual(job.fail('foo', 'Some sort of message'), False)
        self.assertEqual(len(self.client.jobs.failed()), 0)
        job = self.client.jobs[self.q.put(qless.Job, {'test': 'fail_state'}, delay=60)]
        self.assertEqual(job.fail('foo', 'Some sort of message'), False)
        self.assertEqual(len(self.client.jobs.failed()), 0)        
        jid = self.q.put(qless.Job, {'test': 'fail_complete'})
        job = self.q.pop()
        job.complete()
        self.assertEqual(job.fail('foo', 'Some sort of message'), False)
        self.assertEqual(len(self.client.jobs.failed()), 0)
    
    def test_put_failed(self):
        # In this test, we want to make sure that if we put a job
        # that has been failed, we want to make sure that it is
        # no longer reported as failed
        #   1) Put a job
        #   2) Fail that job
        #   3) Make sure we get failed stats
        #   4) Put that job on again
        #   5) Make sure that we no longer get failed stats
        self.assertEqual(len(self.q), 0, 'Start with an empty queue')
        jid = self.q.put(qless.Job, {'test': 'put_failed'})
        job = self.q.pop()
        job.fail('foo', 'some message')
        self.assertEqual(self.client.jobs.failed(), {'foo':1})
        job.move('testing')
        self.assertEqual(len(self.q), 1)
        self.assertEqual(self.client.jobs.failed(), {})
    
    def test_complete_failed(self):
        # No matter if a job has been failed before or not, then we
        # should delete the failure information we have once a job
        # has completed.
        jid = self.q.put(qless.Job, {'test': 'put_failed'})
        job = self.q.pop()
        job.fail('foo', 'some message')
        job.move('testing')
        job = self.q.pop()
        self.assertEqual(job.complete(), 'complete')
        self.assertEqual(self.client.jobs[jid].failure, {})

class TestEverything(TestQless):    
    def test_config(self):
        # Set this particular configuration value
        config = self.client.config
        config.set('testing', 'foo')
        self.assertEqual(config.get('testing'), 'foo')
        # Now let's get all the configuration options and make
        # sure that it's a dictionary, and that it has a key for 'testing'
        self.assertTrue(isinstance(config.get(), dict))
        self.assertEqual(config.get()['testing'], 'foo')
        # Now we'll delete this configuration option and make sure that
        # when we try to get it again, it doesn't exist
        config.set('testing')
        self.assertEqual(config.get('testing'), None)
    
    def test_put_get(self):
        # In this test, I want to make sure that I can put a job into
        # a queue, and then retrieve its data
        #   1) put in a job
        #   2) get job
        #   3) delete job
        jid = self.q.put(qless.Job, {'test': 'put_get'})
        job = self.client.jobs[jid]
        self.assertEqual(job.priority   , 0)
        self.assertEqual(job.data       , {'test': 'put_get'})
        self.assertEqual(job.tags       , [])
        self.assertEqual(job.worker_name, '')
        self.assertEqual(job.state      , 'waiting')
        self.assertEqual(job.klass_name , 'qless.job.Job')
        self.assertEqual(job.klass      , qless.Job)
        # Make sure the times for the history match up
        job.history[0]['put'] = math.floor(job.history[0]['put'])
        self.assertEqual(job.history , [{
            'q'    : 'testing',
            'put'  : math.floor(time.time())
        }])
    
    def test_push_peek_pop_many(self):
        # In this test, we're going to add several jobs, and make
        # sure that they:
        #   1) get put onto the queue
        #   2) we can peek at them
        #   3) we can pop them all off
        #   4) once we've popped them off, we can 
        self.assertEqual(len(self.q), 0, 'Start with an empty queue')
        jids = [self.q.put(qless.Job, {'test': 'push_pop_many', 'count': c}) for c in range(10)]
        self.assertEqual(len(self.q), len(jids), 'Inserting should increase the size of the queue')
        
        # Alright, they're in the queue. Let's take a peek
        self.assertEqual(len(self.q.peek(7)) , 7)
        self.assertEqual(len(self.q.peek(10)), 10)
        
        # Now let's pop them all off one by one
        self.assertEqual(len(self.q.pop(7)) , 7)
        self.assertEqual(len(self.q.pop(10)), 3)
    
    def test_put_pop_attributes(self):
        # In this test, we want to put a job, pop a job, and make
        # sure that when popped, we get all the attributes back 
        # that we expect
        #   1) put a job
        #   2) pop said job, check existence of attributes
        jid = self.q.put(qless.Job, {'test': 'test_put_pop_attributes'})
        self.client.config.set('heartbeat', 60)
        job = self.q.pop()
        self.assertEqual(job.data       , {'test': 'test_put_pop_attributes'})
        self.assertEqual(job.worker_name, self.client.worker_name)
        self.assertTrue( job.ttl        > 0)
        self.assertEqual(job.state      , 'running')
        self.assertEqual(job.queue_name , 'testing')
        self.assertEqual(job.queue.name , 'testing')
        self.assertEqual(job.retries_left , 5)
        self.assertEqual(job.original_retries   , 5)
        self.assertEqual(job.jid        , jid)
        self.assertEqual(job.klass_name , 'qless.job.Job')
        self.assertEqual(job.klass      , qless.Job)
        self.assertEqual(job.tags       , [])
        jid = self.q.put(FooJob, {'test': 'test_put_pop_attributes'})
        job = self.q.pop()
        self.assertTrue('FooJob' in job.klass_name)
    
    def test_data_access(self):
        # In this test, we'd like to make sure that all the data attributes
        # of the job can be accessed through __getitem__
        #   1) Insert a job
        #   2) Get a job,  check job['test']
        #   3) Peek a job, check job['test']
        #   4) Pop a job,  check job['test']
        self.assertEqual(len(self.q), 0, 'Start with an empty queue')
        jid = self.q.put(qless.Job, {'test': 'data_access'})
        job = self.client.jobs[jid]
        self.assertEqual(job['test'], 'data_access')
        job = self.q.peek()
        self.assertEqual(job['test'], 'data_access')
        job = self.q.pop()
        self.assertEqual(job['test'], 'data_access')
    
    def test_put_pop_priority(self):
        # In this test, we're going to add several jobs and make
        # sure that we get them in an order based on priority
        #   1) Insert 10 jobs into the queue with successively more priority
        #   2) Pop all the jobs, and ensure that with each pop we get the right one
        self.assertEqual(len(self.q), 0, 'Start with an empty queue')
        jids = [self.q.put(qless.Job, {'test': 'put_pop_priority', 'count': c}, priority=-c) for c in range(10)]
        last = len(jids)
        for i in range(len(jids)):
            job = self.q.pop()
            self.assertTrue(job['count'] < last, 'We should see jobs in reverse order')
            last = job['count']
    
    def test_same_priority_order(self):
        # In this test, we want to make sure that jobs are popped
        # off in the same order they were put on, priorities being
        # equal.
        #   1) Put some jobs
        #   2) Pop some jobs, save jids
        #   3) Put more jobs
        #   4) Pop until empty, saving jids
        #   5) Ensure popped jobs are in the same order
        jids   = []
        popped = []
        for count in range(100):
            jids.append(self.q.put(qless.Job, {'test': 'put_pop_order', 'count': 2 * count }))
            self.q.peek()
            jids.append(self.q.put(FooJob, {'test': 'put_pop_order', 'count': 2 * count+ 1 }))
            popped.append(self.q.pop().jid)
            self.q.peek()
        
        popped.extend(self.q.pop().jid for i in range(100))
        self.assertEqual(jids, popped)
    
    def test_scheduled(self):
        # In this test, we'd like to make sure that we can't pop
        # off a job scheduled for in the future until it has been
        # considered valid
        #   1) Put a job scheduled for 10s from now
        #   2) Ensure an empty pop
        #   3) 'Wait' 10s
        #   4) Ensure pop contains that job
        # This is /ugly/, but we're going to path the time function so
        # that we can fake out how long these things are waiting
        self.assertEqual(len(self.q), 0, 'Start with an empty queue')
        time.freeze()
        jid = self.q.put(qless.Job, {'test': 'scheduled'}, delay=10)
        self.assertEqual(self.q.pop(), None)
        self.assertEqual(len(self.q), 1)
        time.advance(11)
        job = self.q.pop()
        self.assertNotEqual(job, None)
        self.assertEqual(job.jid, jid)
        time.unfreeze()
    
    def test_scheduled_peek_pop_state(self):
        # Despite the wordy test name, we want to make sure that
        # when a job is put with a delay, that its state is 
        # 'scheduled', when we peek it or pop it and its state is
        # now considered valid, then it should be 'waiting'
        time.freeze()
        jid = self.q.put(qless.Job, {'test': 'scheduled_state'}, delay=10)
        self.assertEqual(self.client.jobs[jid].state, 'scheduled')
        time.advance(11)
        self.assertEqual(self.q.peek().state, 'waiting')
        self.assertEqual(self.client.jobs[jid].state, 'waiting')
        time.unfreeze()
    
    def test_put_pop_complete_history(self):
        # In this test, we want to put a job, pop it, and then 
        # verify that its history has been updated accordingly.
        #   1) Put a job on the queue
        #   2) Get job, check history
        #   3) Pop job, check history
        #   4) Complete job, check history
        self.assertEqual(len(self.q), 0, 'Start with an empty queue')
        jid = self.q.put(qless.Job, {'test': 'put_history'})
        job = self.client.jobs[jid]
        self.assertEqual(math.floor(job.history[0]['put']), math.floor(time.time()))
        # Now pop it
        job = self.q.pop()
        job = self.client.jobs[jid]
        self.assertEqual(math.floor(job.history[0]['popped']), math.floor(time.time()))
    
    def test_move_queue(self):
        # In this test, we want to verify that if we put a job
        # in one queue, and then move it, that it is in fact
        # no longer in the first queue.
        #   1) Put a job in one queue
        #   2) Put the same job in another queue
        #   3) Make sure that it's no longer in the first queue
        self.assertEqual(len(self.q    ), 0, 'Start with an empty queue')
        self.assertEqual(len(self.other), 0, 'Start with an empty queue "other"')
        jid = self.q.put(qless.Job, {'test': 'move_queues'})
        self.assertEqual(len(self.q), 1, 'Put failed')
        job = self.client.jobs[jid]
        job.move('other')
        self.assertEqual(len(self.q    ), 0, 'Move failed')
        self.assertEqual(len(self.other), 1, 'Move failed')
    
    def test_move_queue_popped(self):
        # In this test, we want to verify that if we put a job
        # in one queue, it's popped, and then we move it before
        # it's turned in, then subsequent attempts to renew the
        # lock or complete the work will fail
        #   1) Put job in one queue
        #   2) Pop that job
        #   3) Put job in another queue
        #   4) Verify that heartbeats fail
        self.assertEqual(len(self.q    ), 0, 'Start with an empty queue')
        self.assertEqual(len(self.other), 0, 'Start with an empty queue "other"')
        jid = self.q.put(qless.Job, {'test': 'move_queue_popped'})
        self.assertEqual(len(self.q), 1, 'Put failed')
        job = self.q.pop()
        self.assertNotEqual(job, None)
        # Now move it
        job.move('other')
        self.assertEqual(job.heartbeat(), False)
    
    def test_move_non_destructive(self):
        # In this test, we want to verify that if we move a job
        # from one queue to another, that it doesn't destroy any
        # of the other data that was associated with it. Like 
        # the priority, tags, etc.
        #   1) Put a job in a queue
        #   2) Get the data about that job before moving it
        #   3) Move it 
        #   4) Get the data about the job after
        #   5) Compare 2 and 4  
        self.assertEqual(len(self.q    ), 0, 'Start with an empty queue')
        self.assertEqual(len(self.other), 0, 'Start with an empty queue "other"')
        jid = self.q.put(qless.Job, {'test': 'move_non_destructive'}, tags=['foo', 'bar'], priority=5)
        before = self.client.jobs[jid]
        before.move('other')
        after  = self.client.jobs[jid]
        self.assertEqual(before.tags    , ['foo', 'bar'])
        self.assertEqual(before.priority, 5)
        self.assertEqual(before.tags    , after.tags)
        self.assertEqual(before.data    , after.data)
        self.assertEqual(before.priority, after.priority)
        self.assertEqual(len(after.history), 2)
    
    def test_heartbeat(self):
        # In this test, we want to make sure that we can still 
        # keep our lock on an object if we renew it in time.
        # The gist of this test is:
        #   1) A gets an item, with positive heartbeat
        #   2) B tries to get an item, fails
        #   3) A renews its heartbeat successfully
        self.assertEqual(len(self.a), 0, 'Start with an empty queue')
        jid  = self.q.put(qless.Job, {'test': 'heartbeat'})
        ajob = self.a.pop()
        self.assertNotEqual(ajob, None)
        bjob = self.b.pop()
        self.assertEqual(bjob, None)
        self.assertTrue(isinstance(ajob.heartbeat(), float))
        self.assertTrue(ajob.ttl > 0)
        # Now try setting a queue-specific heartbeat
        self.q.heartbeat = -60
        self.assertTrue(isinstance(ajob.heartbeat(), float))
        self.assertTrue(ajob.ttl <= 0)
    
    def test_heartbeat_expiration(self):
        # In this test, we want to make sure that when we heartbeat a 
        # job, its expiration in the queue is also updated. So, supposing
        # that I heartbeat a job 5 times, then its expiration as far as
        # the lock itself is concerned is also updated
        self.client.config.set('crawl-heartbeat', 7200)
        jid = self.q.put(qless.Job, {})
        job = self.a.pop()
        self.assertEqual(self.b.pop(), None)
        time.freeze()
        # Now, we'll advance the apparent system clock, and heartbeat
        for i in range(10):
            time.advance(3600)
            self.assertNotEqual(job.heartbeat(), False)
            self.assertEqual(self.b.pop(), None)
        
        # Reset it to the original time object
        time.unfreeze()
    
    def test_heartbeat_state(self):
        # In this test, we want to make sure that we cannot heartbeat
        # a job that has not yet been popped
        #   1) Put a job
        #   2) DO NOT pop that job
        #   3) Ensure we cannot heartbeat that job
        self.assertEqual(len(self.q), 0, 'Start with an empty queue')
        jid = self.q.put(qless.Job, {'test': 'heartbeat_state'})
        job = self.client.jobs[jid]
        self.assertEqual(job.heartbeat(), False)
    
    def test_peek_pop_empty(self):
        # Make sure that we can safely pop from an empty queue
        #   1) Make sure the queue is empty
        #   2) When we pop from it, we don't get anything back
        #   3) When we peek, we don't get anything
        self.assertEqual(len(self.q), 0, 'Start with an empty queue')
        self.assertEqual(self.q.pop(), None)
        self.assertEqual(self.q.peek(), None)
    
    def test_peek_attributes(self):
        # In this test, we want to put a job and peek that job, we 
        # get all the attributes back that we expect
        #   1) put a job
        #   2) peek said job, check existence of attributes
        jid = self.q.put(qless.Job, {'test': 'test_put_pop_attributes'})
        job = self.q.peek()
        self.assertEqual(job.data       , {'test': 'test_put_pop_attributes'})
        self.assertEqual(job.worker_name, '')
        self.assertEqual(job.state      , 'waiting')
        self.assertEqual(job.queue_name , 'testing')
        self.assertEqual(job.queue.name , 'testing')
        self.assertEqual(job.retries_left , 5)
        self.assertEqual(job.original_retries   , 5)
        self.assertEqual(job.jid        , jid)
        self.assertEqual(job.klass_name , 'qless.job.Job')
        self.assertEqual(job.klass      , qless.Job)
        self.assertEqual(job.tags       , [])
        jid = self.q.put(FooJob, {'test': 'test_put_pop_attributes'})
        # Pop off the first job
        job = self.q.pop()
        job = self.q.peek()
        self.assertTrue('FooJob' in job.klass_name)
    
    def test_locks(self):
        # In this test, we're going to have two queues that point
        # to the same queue, but we're going to have them represent
        # different workers. The gist of it is this
        #   1) A gets an item, with negative heartbeat
        #   2) B gets the same item,
        #   3) A tries to renew lock on item, should fail
        #   4) B tries to renew lock on item, should succeed
        #   5) Both clean up
        jid = self.q.put(qless.Job, {'test': 'locks'})
        # Reset our heartbeat for both A and B
        self.client.config.set('heartbeat', -10)
        # Make sure a gets a job
        ajob = self.a.pop()
        self.assertNotEqual(ajob, None)
        # Now, make sure that b gets that same job
        bjob = self.b.pop()
        self.assertNotEqual(bjob, None)
        self.assertEqual(ajob.jid, bjob.jid)
        self.assertTrue(isinstance(bjob.heartbeat(), float))
        self.assertTrue((bjob.heartbeat() + 11) >= time.time())
        self.assertEqual(ajob.heartbeat(), False)
    

    
    def test_cancel(self):
        # In this test, we want to make sure that we can corretly
        # cancel a job
        #   1) Put a job
        #   2) Cancel a job
        #   3) Ensure that it's no longer in the queue
        #   4) Ensure that we can't get data for it
        self.assertEqual(len(self.q), 0, 'Start with an empty queue')
        jid = self.q.put(qless.Job, {'test': 'cancel'})
        job = self.client.jobs[jid]
        self.assertEqual(len(self.q), 1)
        job.cancel()
        self.assertEqual(len(self.q), 0)
        self.assertEqual(self.client.jobs[jid], None)
    
    def test_cancel_heartbeat(self):
        # In this test, we want to make sure that when we cancel
        # a job, that heartbeats fail, as do completion attempts
        #   1) Put a job
        #   2) Pop that job
        #   3) Cancel that job
        #   4) Ensure that it's no longer in the queue
        #   5) Heartbeats fail, Complete fails
        #   6) Ensure that we can't get data for it
        self.assertEqual(len(self.q), 0, 'Start with an empty queue')
        jid = self.q.put(qless.Job, {'test': 'cancel_heartbeat'})
        job = self.q.pop()
        job.cancel()
        self.assertEqual(len(self.q), 0)
        self.assertEqual(job.heartbeat(), False)
        self.assertEqual(job.complete() , False)
        self.assertEqual(self.client.jobs[jid], None)
    
    def test_cancel_fail(self):
        # In this test, we want to make sure that if we fail a job
        # and then we cancel it, then we want to make sure that when
        # we ask for what jobs failed, we shouldn't see this one
        #   1) Put a job
        #   2) Fail that job
        #   3) Make sure we see failure stats
        #   4) Cancel that job
        #   5) Make sure that we don't see failure stats
        jid = self.q.put(qless.Job, {'test': 'cancel_fail'})
        job = self.q.pop()
        job.fail('foo', 'some message')
        self.assertEqual(self.client.jobs.failed(), {'foo': 1})
        job.cancel()
        self.assertEqual(self.client.jobs.failed(), {})
        
    def test_complete(self):
        # In this test, we want to make sure that a job that has been
        # completed and not simultaneously enqueued are correctly 
        # marked as completed. It should have a complete history, and
        # have the correct state, no worker, and no queue
        #   1) Put an item in a queue
        #   2) Pop said item from the queue
        #   3) Complete that job
        #   4) Get the data on that job, check state
        self.assertEqual(len(self.q), 0, 'Start with an empty queue')
        jid = self.q.put(qless.Job, {'test': 'complete'})
        job = self.q.pop()
        self.assertNotEqual(job, None)
        self.assertEqual(job.complete(), 'complete')
        job = self.client.jobs[jid]
        self.assertEqual(math.floor(job.history[-1]['done']), math.floor(time.time()))
        self.assertEqual(job.state      , 'complete')
        self.assertEqual(job.worker_name, '')
        self.assertEqual(job.queue_name , '')
        self.assertEqual(len(self.q), 0)
        self.assertEqual(self.client.jobs.complete(), [jid])
        
        # Now, if we move job back into a queue, we shouldn't see any
        # completed jobs anymore
        job.move('testing')
        self.assertEqual(self.client.jobs.complete(), [])
    
    def test_complete_advance(self):
        # In this test, we want to make sure that a job that has been
        # completed and simultaneously enqueued has the correct markings.
        # It shouldn't have a worker, its history should be updated,
        # and the next-named queue should have that item.
        #   1) Put an item in a queue
        #   2) Pop said item from the queue
        #   3) Complete that job, re-enqueueing it
        #   4) Get the data on that job, check state
        #   5) Ensure that there is a work item in that queue
        self.assertEqual(len(self.q), 0, 'Start with an empty queue')
        jid = self.q.put(qless.Job, {'test': 'complete_advance'})
        job = self.q.pop()
        self.assertNotEqual(job, None)
        self.assertEqual(job.complete('testing'), 'waiting')
        job = self.client.jobs[jid]
        self.assertEqual(len(job.history), 2)
        self.assertEqual(math.floor(job.history[-2]['done']), math.floor(time.time()))
        self.assertEqual(math.floor(job.history[-1]['put' ]), math.floor(time.time()))
        self.assertEqual(job.state      , 'waiting')
        self.assertEqual(job.worker_name, '')
        self.assertEqual(job.queue_name , 'testing')
        self.assertEqual(job.queue.name , 'testing')
        self.assertEqual(len(self.q), 1)
    
    def test_complete_fail(self):
        # In this test, we want to make sure that a job that has been
        # handed out to a second worker can both be completed by the
        # second worker, and not completed by the first.
        #   1) Hand a job out to one worker, expire
        #   2) Hand a job out to a second worker
        #   3) First worker tries to complete it, should fail
        #   4) Second worker tries to complete it, should succeed
        self.assertEqual(len(self.q), 0, 'Start with an empty queue')
        jid = self.q.put(qless.Job, {'test': 'complete_fail'})
        self.client.config.set('heartbeat', -10)
        ajob = self.a.pop()
        self.assertNotEqual(ajob, None)
        bjob = self.b.pop()
        self.assertNotEqual(bjob, None)
        self.assertEqual(ajob.complete() , False)
        self.assertEqual(bjob.complete() , 'complete')
        job = self.client.jobs[jid]
        self.assertEqual(math.floor(job.history[-1]['done']), math.floor(time.time()))
        self.assertEqual(job.state      , 'complete')
        self.assertEqual(job.worker_name, '')
        self.assertEqual(job.queue_name , '')
        self.assertEqual(len(self.q), 0)
    
    def test_complete_state(self):
        # In this test, we want to make sure that if we try to complete
        # a job that's in anything but the 'running' state.
        #   1) Put an item in a queue
        #   2) DO NOT pop that item from the queue
        #   3) Attempt to complete the job, ensure it fails
        self.assertEqual(len(self.q), 0, 'Start with an empty queue')
        jid = self.q.put(qless.Job, {'test': 'complete_fail'})
        job = self.client.jobs[jid]
        self.assertEqual(job.complete('testing'), False)
    
    def test_complete_queues(self):
        # In this test, we want to make sure that if we complete a job and
        # advance it, that the new queue always shows up in the 'queues'
        # endpoint.
        #   1) Put an item in a queue
        #   2) Complete it, advancing it to a different queue
        #   3) Ensure it appears in 'queues'
        self.assertEqual(len(self.q), 0, 'Start with an empty queue')
        jid = self.q.put(qless.Job, {'test': 'complete_queues'})
        self.assertEqual(len([q for q in self.client.queues.counts if q['name'] == 'other']), 0)
        self.q.pop().complete('other')
        self.assertEqual(len([q for q in self.client.queues.counts if q['name'] == 'other']), 1)
    
    def test_job_time_expiration(self):
        # In this test, we want to make sure that we honor our job
        # expiration, in the sense that when jobs are completed, we 
        # then delete all the jobs that should be expired according
        # to our deletion criteria
        #   1) First, set jobs-history to -1
        #   2) Then, insert a bunch of jobs
        #   3) Pop each of these jobs
        #   4) Complete each of these jobs
        #   5) Ensure that we have no data about jobs
        self.client.config.set('jobs-history', -1)
        jids = [self.q.put(qless.Job, {'test': 'job_time_experiation', 'count':c}) for c in range(20)]
        for c in range(len(jids)):
            self.q.pop().complete()
        self.assertEqual(self.redis.zcard('ql:completed'), 0)
        self.assertEqual(len(self.redis.keys('ql:j:*')), 0)
    
    def test_job_count_expiration(self):
        # In this test, we want to make sure that we honor our job
        # expiration, in the sense that when jobs are completed, we 
        # then delete all the jobs that should be expired according
        # to our deletion criteria
        #   1) First, set jobs-history-count to 10
        #   2) Then, insert 20 jobs
        #   3) Pop each of these jobs
        #   4) Complete each of these jobs
        #   5) Ensure that we have data about 10 jobs
        self.client.config.set('jobs-history-count', 10)
        jids = [self.q.put(qless.Job, {'test': 'job_count_expiration', 'count':c}) for c in range(20)]
        for c in range(len(jids)):
            self.q.pop().complete()
        self.assertEqual(self.redis.zcard('ql:completed'), 10)
        self.assertEqual(len(self.redis.keys('ql:j:*')), 10)
    
    def test_stats_waiting(self):
        # In this test, we're going to make sure that statistics are
        # correctly collected about how long items wait in a queue
        #   1) Ensure there are no wait stats currently
        #   2) Add a bunch of jobs to a queue
        #   3) Pop a bunch of jobs from that queue, faking out the times
        #   4) Ensure that there are now correct wait stats
        stats = self.q.stats(time.time())
        self.assertEqual(stats['wait']['count'], 0)
        self.assertEqual(stats['run' ]['count'], 0)
        
        time.freeze()
        jids = [self.q.put(qless.Job, {'test': 'stats_waiting', 'count': c}) for c in range(20)]
        self.assertEqual(len(jids), 20)
        for i in range(len(jids)):
            self.assertNotEqual(self.q.pop(), None)
            time.advance(1)
        
        time.unfreeze()
        # Now, make sure that we see stats for the waiting
        stats = self.q.stats(time.time())
        self.assertEqual(stats['wait']['count'], 20)
        self.assertEqual(stats['wait']['mean'] , 9.5)
        # This is our expected standard deviation
        self.assertTrue(stats['wait']['std'] - 5.916079783099 < 1e-8)
        # Now make sure that our histogram looks like what we think it
        # should
        self.assertEqual(stats['wait']['histogram'][0:20], [1] * 20)
        self.assertEqual(sum(stats['run' ]['histogram']), stats['run' ]['count'])
        self.assertEqual(sum(stats['wait']['histogram']), stats['wait']['count'])
    
    def test_stats_complete(self):
        # In this test, we want to make sure that statistics are
        # correctly collected about how long items take to actually 
        # get processed.
        #   1) Ensure there are no run stats currently
        #   2) Add a bunch of jobs to a queue
        #   3) Pop those jobs
        #   4) Complete those jobs, faking out the time
        #   5) Ensure that there are now correct run stats
        stats = self.q.stats(time.time())
        self.assertEqual(stats['wait']['count'], 0)
        self.assertEqual(stats['run' ]['count'], 0)
        
        time.freeze()
        jids = [self.q.put(qless.Job, {'test': 'stats_waiting', 'count': c}) for c in range(20)]
        jobs = self.q.pop(20)
        self.assertEqual(len(jobs), 20)
        for job in jobs:
            job.complete()
            time.advance(1)
        
        time.unfreeze()
        # Now, make sure that we see stats for the waiting
        stats = self.q.stats(time.time())
        self.assertEqual(stats['run']['count'], 20)
        self.assertEqual(stats['run']['mean'] , 9.5)
        # This is our expected standard deviation
        self.assertTrue(stats['run']['std'] - 5.916079783099 < 1e-8)
        # Now make sure that our histogram looks like what we think it
        # should
        self.assertEqual(stats['run']['histogram'][0:20], [1] * 20)
        self.assertEqual(sum(stats['run' ]['histogram']), stats['run' ]['count'])
        self.assertEqual(sum(stats['wait']['histogram']), stats['wait']['count'])
    
    def test_queues(self):
        # In this test, we want to make sure that the queues function
        # can correctly identify the numbers associated with that queue
        #   1) Make sure we get nothing for no queues
        #   2) Put delayed item, check
        #   3) Put item, check
        #   4) Put, pop item, check
        #   5) Put, pop, lost item, check
        self.assertEqual(len(self.q), 0)
        self.assertEqual(self.client.queues.counts, {})
        # Now, let's actually add an item to a queue, but scheduled
        self.q.put(qless.Job, {'test': 'queues'}, delay=10)
        expected = {
            'name': 'testing',
            'stalled': 0,
            'waiting': 0,
            'running': 0,
            'scheduled': 1,
            'depends': 0,
            'recurring': 0
        }
        self.assertEqual(self.client.queues.counts, [expected])
        self.assertEqual(self.client.queues["testing"].counts, expected)
        
        self.q.put(qless.Job, {'test': 'queues'})
        expected['waiting'] += 1
        self.assertEqual(self.client.queues.counts, [expected])
        self.assertEqual(self.client.queues["testing"].counts, expected)
        
        job = self.q.pop()
        expected['waiting'] -= 1
        expected['running'] += 1
        self.assertEqual(self.client.queues.counts, [expected])
        self.assertEqual(self.client.queues["testing"].counts, expected)
        
        # Now we'll have to mess up our heartbeat to make this work
        self.q.put(qless.Job, {'test': 'queues'})
        self.client.config.set('heartbeat', -10)
        job = self.q.pop()
        expected['stalled'] += 1
        self.assertEqual(self.client.queues.counts, [expected])
        self.assertEqual(self.client.queues["testing"].counts, expected)
    
    def test_track(self):
        # In this test, we want to make sure that tracking works as expected.
        #   1) Check tracked jobs, expect none
        #   2) Put, Track a job, check
        #   3) Untrack job, check
        #   4) Track job, cancel, check
        self.assertEqual(self.client.jobs.tracked(), {'expired':{}, 'jobs':[]})
        job = self.client.jobs[self.q.put(qless.Job, {'test':'track'})]
        job.track()
        self.assertEqual(len(self.client.jobs.tracked()['jobs']), 1)
        job.untrack()
        self.assertEqual(len(self.client.jobs.tracked()['jobs']), 0)
        job.track()
        job.cancel()
        self.assertEqual(len(self.client.jobs.tracked()['expired']), 1)
    
    def test_track_tracked(self):
        # When peeked, popped, failed, etc., qless should know when a 
        # job is tracked or not
        # => 1) Put a job, track it
        # => 2) Peek, ensure tracked
        # => 3) Pop, ensure tracked
        # => 4) Fail, check failed, ensure tracked
        job = self.client.jobs[self.q.put(qless.Job, {'test': 'track_tracked'})]
        job.track()
        self.assertEqual(self.q.peek().tracked, True)
        job = self.q.pop()
        self.assertEqual(job.tracked, True)
        job.fail('foo', 'bar')
        self.assertEqual(self.client.jobs.failed('foo')['jobs'][0].tracked, True)
    
    def test_track_untracked(self):
        # When peeked, popped, failed, etc., qless should know when a 
        # job is not tracked
        # => 1) Put a job
        # => 2) Peek, ensure tracked
        # => 3) Pop, ensure tracked
        # => 4) Fail, check failed, ensure tracked
        job = self.client.jobs[self.q.put(qless.Job, {'test': 'track_tracked'})]
        self.assertEqual(self.q.peek().tracked, False)
        job = self.q.pop()
        self.assertEqual(job.tracked, False)
        job.fail('foo', 'bar')
        self.assertEqual(self.client.jobs.failed('foo')['jobs'][0].tracked, False)
    
    def test_retries(self):
        # In this test, we want to make sure that jobs are given a
        # certain number of retries before automatically being considered
        # failed.
        #   1) Put a job with a few retries
        #   2) Verify there are no failures
        #   3) Lose the heartbeat as many times
        #   4) Verify there are failures
        #   5) Verify the queue is empty
        self.assertEqual(self.client.jobs.failed(), {})
        self.q.put(qless.Job, {'test':'retries'}, retries=2)
        # Easier to lose the heartbeat lock
        self.client.config.set('heartbeat', -10)
        self.assertNotEqual(self.q.pop(), None)
        self.assertEqual(self.client.jobs.failed(), {})
        self.assertNotEqual(self.q.pop(), None)
        self.assertEqual(self.client.jobs.failed(), {})
        self.assertNotEqual(self.q.pop(), None)
        self.assertEqual(self.client.jobs.failed(), {})
        # This one should do it
        self.assertEqual(self.q.pop(), None)
        self.assertEqual(self.client.jobs.failed(), {'failed-retries-testing':1})
    
    def test_retries_complete(self):
        # In this test, we want to make sure that jobs have their number
        # of remaining retries reset when they are put on a new queue
        #   1) Put an item with 2 retries
        #   2) Lose the heartbeat once
        #   3) Get the job, make sure it has 1 remaining
        #   4) Complete the job
        #   5) Get job, make sure it has 2 remaining
        jid = self.q.put(qless.Job, {'test':'retries_complete'}, retries=2)
        self.client.config.set('heartbeat', -10)
        job = self.q.pop()
        self.assertNotEqual(job, None)
        job = self.q.pop()
        self.assertEqual(job.retries_left, 1)
        job.complete()
        job = self.client.jobs[jid]
        self.assertEqual(job.retries_left, 2)
    
    def test_retries_put(self):
        # In this test, we want to make sure that jobs have their number
        # of remaining retries reset when they are put on a new queue
        #   1) Put an item with 2 retries
        #   2) Lose the heartbeat once
        #   3) Get the job, make sure it has 1 remaining
        #   4) Re-put the job in the queue with job.move
        #   5) Get job, make sure it has 2 remaining
        jid = self.q.put(qless.Job, {'test':'retries_put'}, retries=2)
        self.client.config.set('heartbeat', -10)
        job = self.q.pop()
        self.assertNotEqual(job, None)
        job = self.q.pop()
        self.assertEqual(job.retries_left, 1)
        job.move('testing')
        job = self.client.jobs[jid]
        self.assertEqual(job.retries_left, 2)
    
    def test_stats_failed(self):
        # In this test, we want to make sure that statistics are
        # correctly collected about how many items are currently failed
        #   1) Put an item
        #   2) Ensure we don't have any failed items in the stats for that queue
        #   3) Fail that item
        #   4) Ensure that failures and failed both increment
        #   5) Put that item back
        #   6) Ensure failed decremented, failures untouched
        jid = self.q.put(qless.Job, {'test':'stats_failed'})
        stats = self.q.stats()
        self.assertEqual(stats['failed'  ], 0)
        self.assertEqual(stats['failures'], 0)
        job = self.q.pop()
        job.fail('foo', 'bar')
        stats = self.q.stats()
        self.assertEqual(stats['failed'  ], 1)
        self.assertEqual(stats['failures'], 1)
        job.move('testing')
        stats = self.q.stats()
        self.assertEqual(stats['failed'  ], 0)
        self.assertEqual(stats['failures'], 1)
    
    def test_stats_retries(self):
        # In this test, we want to make sure that retries are getting
        # captured correctly in statistics
        #   1) Put a job
        #   2) Pop job, lose lock
        #   3) Ensure no retries in stats
        #   4) Pop job,
        #   5) Ensure one retry in stats
        jid = self.q.put(qless.Job, {'test':'stats_retries'})
        self.client.config.set('heartbeat', -10)
        job = self.q.pop()
        self.assertEqual(self.q.stats()['retries'], 0)
        job = self.q.pop()
        self.assertEqual(self.q.stats()['retries'], 1)
    
    def test_stats_failed_original_day(self):
        # In this test, we want to verify that if we unfail a job on a
        # day other than the one on which it originally failed, that we
        # the `failed` stats for the original day are decremented, not
        # today.
        #   1) Put a job
        #   2) Fail that job
        #   3) Advance the clock 24 hours
        #   4) Put the job back
        #   5) Check the stats with today, check failed = 0, failures = 0
        #   6) Check 'yesterdays' stats, check failed = 0, failures = 1
        jid = self.q.put(qless.Job, {'test':'stats_failed_original_day'})
        job = self.q.pop()
        job.fail('foo', 'bar')
        stats = self.q.stats()
        self.assertEqual(stats['failures'], 1)
        self.assertEqual(stats['failed'  ], 1)
        
        time.freeze(); time.advance(86400)
        job.move('testing')
        # Now check tomorrow's stats
        today = self.q.stats()
        self.assertEqual(today['failures'], 0)
        self.assertEqual(today['failed'  ], 0)
        time.unfreeze()
        yesterday = self.q.stats()
        self.assertEqual(yesterday['failures'], 1)
        self.assertEqual(yesterday['failed']  , 0)
    
    def test_workers(self):
        # In this test, we want to verify that when we add a job, we 
        # then know about that worker, and that it correctly identifies
        # the jobs it has.
        #   1) Put a job
        #   2) Ensure empty 'workers'
        #   3) Pop that job
        #   4) Ensure unempty 'workers'
        #   5) Ensure unempty 'worker'
        jid = self.q.put(qless.Job, {'test':'workers'})
        self.assertEqual(self.client.workers.all(), {})
        job = self.q.pop()
        self.assertEqual(self.client.workers.all(), [{
            'name'   : self.q.worker_name,
            'jobs'   : 1,
            'stalled': 0
        }])
        # Now get specific worker information
        self.assertEqual(self.client.workers[self.q.worker_name], {
            'jobs'   : [jid],
            'stalled': []
        })
    
    def test_workers_cancel(self):
        # In this test, we want to verify that when a job is canceled,
        # that it is removed from the list of jobs associated with a worker
        #   1) Put a job
        #   2) Pop that job
        #   3) Ensure 'workers' and 'worker' know about it
        #   4) Cancel job
        #   5) Ensure 'workers' and 'worker' reflect that
        jid = self.q.put(qless.Job, {'test':'workers_cancel'})
        job = self.q.pop()
        self.assertEqual(self.client.workers.all(), [{
            'name'   : self.q.worker_name,
            'jobs'   : 1,
            'stalled': 0
        }])
        self.assertEqual(self.client.workers[self.q.worker_name], {
            'jobs'   : [jid],
            'stalled': []
        })
        # Now cancel the job
        job.cancel()
        self.assertEqual(self.client.workers.all(), [{
            'name'   : self.q.worker_name,
            'jobs'   : 0,
            'stalled': 0
        }])
        self.assertEqual(self.client.workers[self.q.worker_name], {
            'jobs'   : [],
            'stalled': []
        })
    
    def test_workers_lost_lock(self):
        # In this test, we want to verify that 'workers' and 'worker'
        # correctly identify that a job is stalled, and that when that
        # job is taken from the lost lock, that it's no longer listed
        # as stalled under the original worker. Also, that workers are
        # listed in order of recency of contact
        #   1) Put a job
        #   2) Pop a job, with negative heartbeat
        #   3) Ensure 'workers' and 'worker' show it as stalled
        #   4) Pop the job with a different worker
        #   5) Ensure 'workers' and 'worker' reflect that
        jid = self.q.put(qless.Job, {'test':'workers_lost_lock'})
        self.client.config.set('heartbeat', -10)
        job = self.q.pop()
        self.assertEqual(self.client.workers.all(), [{
            'name'   : self.q.worker_name,
            'jobs'   : 0,
            'stalled': 1
        }])
        self.assertEqual(self.client.workers[self.q.worker_name], {
            'jobs'   : [],
            'stalled': [jid]
        })
        # Now, let's pop it with a different worker
        self.client.config.set('heartbeat')
        job = self.a.pop()
        self.assertEqual(self.client.workers.all(), [{
            'name'   : self.a.worker_name,
            'jobs'   : 1,
            'stalled': 0
        }, {
            'name'   : self.q.worker_name,
            'jobs'   : 0,
            'stalled': 0
        }])
        self.assertEqual(self.client.workers[self.q.worker_name], {
            'jobs'   : [],
            'stalled': []
        })
    
    def test_workers_fail(self):
        # In this test, we want to make sure that when we fail a job,
        # its reflected correctly in 'workers' and 'worker'
        #   1) Put a job
        #   2) Pop job, check 'workers', 'worker'
        #   3) Fail that job
        #   4) Check 'workers', 'worker'
        jid = self.q.put(qless.Job, {'test':'workers_fail'})
        job = self.q.pop()
        self.assertEqual(self.client.workers.all(), [{
            'name'   : self.q.worker_name,
            'jobs'   : 1,
            'stalled': 0
        }])
        self.assertEqual(self.client.workers[self.q.worker_name], {
            'jobs'   : [jid],
            'stalled': []
        })
        # Now, let's fail it
        job.fail('foo', 'bar')
        self.assertEqual(self.client.workers.all(), [{
            'name'   : self.q.worker_name,
            'jobs'   : 0,
            'stalled': 0
        }])
        self.assertEqual(self.client.workers[self.q.worker_name], {
            'jobs'   : [],
            'stalled': []
        })
    
    def test_workers_complete(self):
        # In this test, we want to make sure that when we complete a job,
        # it's reflected correctly in 'workers' and 'worker'
        #   1) Put a job
        #   2) Pop a job, check 'workers', 'worker'
        #   3) Complete job, check 'workers', 'worker'
        jid = self.q.put(qless.Job, {'test':'workers_complete'})
        job = self.q.pop()
        self.assertEqual(self.client.workers.all(), [{
            'name'   : self.q.worker_name,
            'jobs'   : 1,
            'stalled': 0
        }])
        self.assertEqual(self.client.workers[self.q.worker_name], {
            'jobs'   : [jid],
            'stalled': []
        })
        # Now complete it
        job.complete()
        self.assertEqual(self.client.workers.all(), [{
            'name'   : self.q.worker_name,
            'jobs'   : 0,
            'stalled': 0
        }])
        self.assertEqual(self.client.workers[self.q.worker_name], {
            'jobs'   : [],
            'stalled': []
        })
    
    def test_workers_reput(self):
        # Make sure that if we move a job from one queue to another, that 
        # the job is no longer listed as one of the jobs that the worker
        # has.
        #   1) Put a job
        #   2) Pop job, check 'workers', 'worker'
        #   3) Move job, check 'workers', 'worker'
        jid = self.q.put(qless.Job, {'test':'workers_reput'})
        job = self.q.pop()
        self.assertEqual(self.client.workers.all(), [{
            'name'   : self.q.worker_name,
            'jobs'   : 1,
            'stalled': 0
        }])
        self.assertEqual(self.client.workers[self.q.worker_name], {
            'jobs'   : [jid],
            'stalled': []
        })
        job.move('other')
        self.assertEqual(self.client.workers.all(), [{
            'name'   : self.q.worker_name,
            'jobs'   : 0,
            'stalled': 0
        }])
        self.assertEqual(self.client.workers[self.q.worker_name], {
            'jobs'   : [],
            'stalled': []
        })
    
    def test_running_stalled_scheduled_depends(self):
        # Make sure that we can get a list of jids for a queue that
        # are running, stalled and scheduled
        #   1) Put a job, pop it, check 'running'
        #   2) Put a job scheduled, check 'scheduled'
        #   3) Put a job with negative heartbeat, pop, check stalled
        #   4) Put a job dependent on another and check 'depends'
        self.assertEqual(len(self.q), 0)
        # Now, we need to check pagination
        jids = [self.q.put(qless.Job, {'test': 'rssd'}) for i in range(20)]
        self.client.config.set('heartbeat', -60)
        jobs = self.q.pop(20)
        self.assertEqual(set(self.q.jobs.stalled(0, 10) + self.q.jobs.stalled(10, 10)), set(jids))
        
        self.client.config.set('heartbeat', 60)
        jobs = self.q.pop(20)
        self.assertEqual(set(self.q.jobs.running(0, 10) + self.q.jobs.running(10, 10)), set(jids))
        
        # Complete all these jobs
        r = [job.complete() for job in jobs]
        jids = [job.jid for job in jobs]
        jids.reverse()
        self.assertEqual(self.client.jobs.complete(0, 10) + self.client.jobs.complete(10, 10), jids)
        
        jids = [self.q.put(qless.Job, {'test': 'rssd'}, delay=60) for i in range(20)]
        self.assertEqual(set(self.q.jobs.scheduled(0, 10) + self.q.jobs.scheduled(10, 10)), set(jids))
        
        jids = [self.q.put(qless.Job, {'test': 'rssd'}, depends=jids) for i in range(20)]
        self.assertEqual(set(self.q.jobs.depends(0, 10) + self.q.jobs.depends(10, 10)), set(jids))
    
    # ==================================================================
    # In these tests, we want to ensure that if we don't provide enough
    # or correctly-formatted arguments to the lua scripts, that they'll
    # barf on us like we ask.
    # ==================================================================
    def test_lua_cancel(self):
        cancel = qless.lua('cancel', self.redis)
        # Providing in keys
        self.assertRaises(Exception, cancel, *(['foo'], ['deadbeef']))
        # Missing an id
        self.assertRaises(Exception, cancel, *([], []))
    
    def test_lua_complete(self):
        complete = qless.lua('complete', self.redis)
        # Not enough args
        self.assertRaises(Exception, complete, *([], []))
        # Providing a key, but shouldn't
        self.assertRaises(Exception, complete, *(['foo'], ['deadbeef', 'worker1', 'foo', 12345]))
        # Missing worker
        self.assertRaises(Exception, complete, *([], ['deadbeef']))
        # Missing queue
        self.assertRaises(Exception, complete, *([], ['deadbeef', 'worker1']))
        # Missing now
        self.assertRaises(Exception, complete, *([], ['deadbeef', 'worker1', 'foo']))
        # Malformed now
        self.assertRaises(Exception, complete, *([], ['deadbeef', 'worker1', 'foo', 'howdy']))
        # Malformed JSON
        self.assertRaises(Exception, complete, *([], ['deadbeef', 'worker1', 'foo', 12345, '[}']))
        # Not a number for delay
        self.assertRaises(Exception, complete, *([], ['deadbeef', 'worker1', 'foo', 12345, '{}', 'next', 'howdy', 'delay', 'howdy']))
        # Mutually exclusive options
        self.assertRaises(Exception, complete, *([], ['deadbeef', 'worker1', 'foo', 12345, '{}', 'next', 'foo', 'delay', 5, 'depends', '["foo"]']))
        # Mutually inclusive options (with 'next')
        self.assertRaises(Exception, complete, *([], ['deadbeef', 'worker1', 'foo', 12345, '{}', 'delay', 5]))
        self.assertRaises(Exception, complete, *([], ['deadbeef', 'worker1', 'foo', 12345, '{}', 'depends', '["foo"]']))
    
    def test_lua_fail(self):
        fail = qless.lua('fail', self.redis)
        # Passing in keys
        self.assertRaises(Exception, fail, *(['foo'], ['deadbeef', 'worker1', 'foo', 'bar', 12345]))
        # Missing id
        self.assertRaises(Exception, fail, *([], []))
        # Missing worker
        self.assertRaises(Exception, fail, *([], ['deadbeef']))
        # Missing type
        self.assertRaises(Exception, fail, *([], ['deadbeef', 'worker1']))
        # Missing message
        self.assertRaises(Exception, fail, *([], ['deadbeef', 'worker1', 'foo']))
        # Missing now
        self.assertRaises(Exception, fail, *([], ['deadbeef', 'worker1', 'foo', 'bar']))
        # Malformed now
        self.assertRaises(Exception, fail, *([], ['deadbeef', 'worker1', 'foo', 'bar', 'howdy']))
        # Malformed data
        self.assertRaises(Exception, fail, *([], ['deadbeef', 'worker1', 'foo', 'bar', 12345, '[}']))
    
    def test_lua_failed(self):
        failed = qless.lua('failed', self.redis)
        # Passing in keys
        self.assertRaises(Exception, failed, *(['foo'], []))
        # Malformed start
        self.assertRaises(Exception, failed, *(['foo'], ['bar', 'howdy']))
        # Malformed limit
        self.assertRaises(Exception, failed, *(['foo'], ['bar', 0, 'howdy']))
    
    def test_lua_get(self):
        get = qless.lua('get', self.redis)
        # Passing in keys
        self.assertRaises(Exception, get, *(['foo'], ['deadbeef']))
        # Missing id
        self.assertRaises(Exception, get, *([], []))
    
    def test_lua_getconfig(self):
        getconfig = qless.lua('getconfig', self.redis)
        # Passing in keys
        self.assertRaises(Exception, getconfig, *(['foo']))
    
    def test_lua_heartbeat(self):
        heartbeat = qless.lua('heartbeat', self.redis)
        # Passing in keys
        self.assertRaises(Exception, heartbeat, *(['foo'], ['deadbeef', 'foo', 12345]))
        # Missing id
        self.assertRaises(Exception, heartbeat, *([], []))
        # Missing worker
        self.assertRaises(Exception, heartbeat, *([], ['deadbeef']))
        # Missing expiration
        self.assertRaises(Exception, heartbeat, *([], ['deadbeef', 'worker1']))
        # Malformed now
        self.assertRaises(Exception, heartbeat, *([], ['deadbeef', 'worker1', 'howdy']))
        # Malformed JSON
        self.assertRaises(Exception, heartbeat, *([], ['deadbeef', 'worker1', 12345, '[}']))
    
    def test_lua_jobs(self):
        jobs = qless.lua('jobs', self.redis)
        # Providing keys
        self.assertRaises(Exception, jobs, *(['foo'], []))
        # Unrecognized option
        self.assertRaises(Exception, jobs, *([], ['testing']))
        # Missing now
        self.assertRaises(Exception, jobs, *([], ['stalled']))
        # Malformed now
        self.assertRaises(Exception, jobs, *([], ['stalled', 'foo']))
        # Missing queue
        self.assertRaises(Exception, jobs, *([], ['stalled', 12345]))
    
    def test_lua_peek(self):
        peek = qless.lua('peek', self.redis)
        # Passing in no keys
        self.assertRaises(Exception, peek, *([], [1, 12345]))
        # Passing in too many keys
        self.assertRaises(Exception, peek, *(['foo', 'bar'], [1, 12345]))
        # Missing count
        self.assertRaises(Exception, peek, *(['foo'], []))
        # Malformed count
        self.assertRaises(Exception, peek, *(['foo'], ['howdy']))
        # Missing now
        self.assertRaises(Exception, peek, *(['foo'], [1]))
        # Malformed now
        self.assertRaises(Exception, peek, *(['foo'], [1, 'howdy']))
    
    def test_lua_pop(self):
        pop = qless.lua('pop', self.redis)
        # Passing in no keys
        self.assertRaises(Exception, pop, *([], ['worker1', 1, 12345, 12346]))
        # Passing in too many keys
        self.assertRaises(Exception, pop, *(['foo', 'bar'], ['worker1', 1, 12345, 12346]))
        # Missing worker
        self.assertRaises(Exception, pop, *(['foo'], []))
        # Missing count
        self.assertRaises(Exception, pop, *(['foo'], ['worker1']))
        # Malformed count
        self.assertRaises(Exception, pop, *(['foo'], ['worker1', 'howdy']))
        # Missing now
        self.assertRaises(Exception, pop, *(['foo'], ['worker1', 1]))
        # Malformed now
        self.assertRaises(Exception, pop, *(['foo'], ['worker1', 1, 'howdy']))
    
    def test_lua_priority(self):
        priority = qless.lua('priority', self.redis)
        # Passing in keys
        self.assertRaises(Exception, priority, *(['foo'], ['12345', 1]))
        # Missing jid
        self.assertRaises(Exception, priority, *([], []))
        # Missing priority
        self.assertRaises(Exception, priority, *([], ['12345']))
        # Malformed priority
        self.assertRaises(Exception, priority, *([], ['12345', 'howdy']))
    
    def test_lua_put(self):
        put = qless.lua('put', self.redis)
        # Passing in no keys
        self.assertRaises(Exception, put, *([], ['deadbeef', '{}', 12345]))
        # Passing in two keys
        self.assertRaises(Exception, put, *(['foo', 'bar'], ['deadbeef', '{}', 12345]))
        # Missing id
        self.assertRaises(Exception, put, *(['foo'], []))
        # Missing klass
        self.assertRaises(Exception, put, *(['foo'], ['deadbeef']))
        # Missing data
        self.assertRaises(Exception, put, *(['foo'], ['deadbeef', 'foo']))
        # Malformed data
        self.assertRaises(Exception, put, *(['foo'], ['deadbeef', 'foo', '[}']))
        # Non-dictionary data
        self.assertRaises(Exception, put, *(['foo'], ['deadbeef', 'foo', '[]']))
        # Non-dictionary data
        self.assertRaises(Exception, put, *(['foo'], ['deadbeef', 'foo', '"foobar"']))
        # Missing now
        self.assertRaises(Exception, put, *(['foo'], ['deadbeef', 'foo', '{}']))
        # Malformed now
        self.assertRaises(Exception, put, *(['foo'], ['deadbeef', 'foo', '{}', 'howdy']))
        # Malformed delay
        self.assertRaises(Exception, put, *(['foo'], ['deadbeef', 'foo', '{}', 12345, 'howdy']))
        # Malformed priority
        self.assertRaises(Exception, put, *(['foo'], ['deadbeef', 'foo', '{}', 12345, 0, 'priority', 'howdy']))
        # Malformed tags
        self.assertRaises(Exception, put, *(['foo'], ['deadbeef', 'foo', '{}', 12345, 0, 'tags', '[}']))
        # Malformed retries
        self.assertRaises(Exception, put, *(['foo'], ['deadbeef', 'foo', '{}', 12345, 0, 'retries', 'hello']))
        # Mutually exclusive options
        self.assertRaises(Exception, put, *(['foo'], ['deadbeef', 'foo', '{}', 12345, 5, 'depends', '["hello"]']))
    
    def test_lua_queues(self):
        queues = qless.lua('queues', self.redis)
        # Passing in keys
        self.assertRaises(Exception, queues, *(['foo'], [12345]))
        # Missing time
        self.assertRaises(Exception, queues, *([], []))
        # Malformed time
        self.assertRaises(Exception, queues, *([], ['howdy']))
    
    def test_lua_queues(self):
        recur = qless.lua('recur', self.redis)
        # Passing in keys
        self.assertRaises(Exception, recur, *(['foo'], [12345]))
        # Missing command, queue, jid, klass, data, now, 'interval', interval, offset
        self.assertRaises(Exception, recur, *([], []))
        self.assertRaises(Exception, recur, *([], ['on']))
        self.assertRaises(Exception, recur, *([], ['on', 'testing']))
        self.assertRaises(Exception, recur, *([], ['on', 'testing', 12345]))
        self.assertRaises(Exception, recur, *([], ['on', 'testing', 12345, 'foo.klass']))
        self.assertRaises(Exception, recur, *([], ['on', 'testing', 12345, 'foo.klass', '{}']))
        self.assertRaises(Exception, recur, *([], ['on', 'testing', 12345, 'foo.klass', '{}', 12345]))
        self.assertRaises(Exception, recur, *([], ['on', 'testing', 12345, 'foo.klass', '{}', 12345, 'interval']))
        self.assertRaises(Exception, recur, *([], ['on', 'testing', 12345, 'foo.klass', '{}', 12345, 'interval', 12345]))
        self.assertRaises(Exception, recur, *([], ['on', 'testing', 12345, 'foo.klass', '{}', 12345, 'interval', 12345, 0]))
        # Malformed data, priority, tags, retries
        self.assertRaises(Exception, recur, *([], ['on', 'testing', 12345, 'foo.klass', '[}', 12345, 'interval', 12345, 0]))
        self.assertRaises(Exception, recur, *([], ['on', 'testing', 12345, 'foo.klass', '{}', 12345, 'interval', 12345, 0, 'priority', 'foo']))
        self.assertRaises(Exception, recur, *([], ['on', 'testing', 12345, 'foo.klass', '{}', 12345, 'interval', 12345, 0, 'retries', 'foo']))
        self.assertRaises(Exception, recur, *([], ['on', 'testing', 12345, 'foo.klass', '{}', 12345, 'interval', 12345, 0, 'tags', '[}']))
        # Missing jid
        self.assertRaises(Exception, recur, *([], ['off']))
        self.assertRaises(Exception, recur, *([], ['get']))
        self.assertRaises(Exception, recur, *([], ['update']))
        self.assertRaises(Exception, recur, *([], ['tag']))
        self.assertRaises(Exception, recur, *([], ['untag']))
        # Malformed priority, interval, retries, data
        self.assertRaises(Exception, recur, *([], ['update', 12345, 'priority', 'foo']))
        self.assertRaises(Exception, recur, *([], ['update', 12345, 'interval', 'foo']))
        self.assertRaises(Exception, recur, *([], ['update', 12345, 'retries', 'foo']))
        self.assertRaises(Exception, recur, *([], ['update', 12345, 'data', '[}']))
    
    def test_lua_retry(self):
        retry = qless.lua('retry', self.redis)
        # Passing in keys
        self.assertRaises(Exception, retry, *(['foo'], ['12345', 'testing', 'worker', 12345, 0]))
        # Missing jid
        self.assertRaises(Exception, retry, *([], []))
        # Missing queue
        self.assertRaises(Exception, retry, *([], ['12345']))
        # Missing worker
        self.assertRaises(Exception, retry, *([], ['12345', 'testing']))
        # Missing now
        self.assertRaises(Exception, retry, *([], ['12345', 'testing', 'worker']))
        # Malformed now
        self.assertRaises(Exception, retry, *([], ['12345', 'testing', 'worker', 'howdy']))
        # Malformed delay
        self.assertRaises(Exception, retry, *([], ['12345', 'testing', 'worker', 12345, 'howdy']))
    
    def test_lua_setconfig(self):
        setconfig = qless.lua('setconfig', self.redis)
        # Passing in keys
        self.assertRaises(Exception, setconfig, *(['foo'], []))
    
    def test_lua_stats(self):
        stats = qless.lua('stats', self.redis)
        # Passing in keys
        self.assertRaises(Exception, stats, *(['foo'], ['foo', 'bar']))
        # Missing queue
        self.assertRaises(Exception, stats, *([], []))
        # Missing date
        self.assertRaises(Exception, stats, *([], ['foo']))
    
    def test_lua_tag(self):
        tag = qless.lua('tag', self.redis)
        # Passing in keys
        self.assertRaises(Exception, tag, *(['foo'], ['add', '12345', 12345, 'foo']))
        # First, test 'add' command
        # Missing command
        self.assertRaises(Exception, tag, *([], []))
        # Missing jid
        self.assertRaises(Exception, tag, *([], ['add']))
        # Missing now
        self.assertRaises(Exception, tag, *([], ['add', '12345']))
        # Malformed now
        self.assertRaises(Exception, tag, *([], ['add', '12345', 'howdy']))
        # Now, test 'remove' command
        # Missing jid
        self.assertRaises(Exception, tag, *([], ['remove']))
        # Now, test 'get'
        # Missing tag
        self.assertRaises(Exception, tag, *([], ['get']))
        # Malformed offset
        self.assertRaises(Exception, tag, *([], ['get', 'foo', 'howdy']))
        # Malformed count
        self.assertRaises(Exception, tag, *([], ['get', 'foo', 0, 'howdy']))
    
    def test_lua_track(self):
        track = qless.lua('track', self.redis)
        # Passing in keys
        self.assertRaises(Exception, track, *(['foo'], []))
        # Unknown command
        self.assertRaises(Exception, track, *([], ['fslkdjf', 'deadbeef', 12345]))
        # Missing jid
        self.assertRaises(Exception, track, *([], ['track']))
        # Missing time
        self.assertRaises(Exception, track, *([], ['track', 'deadbeef']))
        # Malformed time
        self.assertRaises(Exception, track, *([], ['track', 'deadbeef', 'howdy']))

#################################
# All the python-client-specific tests
#################################
class BarJob(object):
    @staticmethod
    def other(job):
        job.fail('foo', 'bar')
    
    @staticmethod
    def process(job):
        job.complete()

class FailJob(object):
    @staticmethod
    def testing(job):
        raise Exception('foo')
    
    # This isn't a static method like it should be
    def other(self, job):
        job.complete()

class MissingJob(object):
    # This class doesn't have any appropriate methods
    pass

class TestPython(TestQless):
    def test_job(self):
        job = self.client.jobs[self.q.put(qless.Job, {})]
        self.assertTrue(job.jid in str(job))
        self.assertTrue(job.jid in repr(job))
        self.assertRaises(AttributeError, lambda: job.foo)
        job['testing'] = 'foo'
        self.assertEqual(job['testing'], 'foo')
    
    def test_queue(self):
        self.assertRaises(AttributeError, lambda: self.q.foo)
    
    def test_process(self):
        jid = self.q.put(BarJob, {})
        self.q.pop().process()
        self.assertEqual(self.client.jobs[jid].state, 'complete')
        
        jid = self.other.put(BarJob, {})
        self.other.pop().process()
        self.assertEqual(self.client.jobs[jid].state, 'failed')
        
        jid = self.q.put(FailJob, {})
        self.q.pop().process()
        self.assertEqual(self.client.jobs[jid].state, 'failed')
        
        jid = self.other.put(FailJob, {})
        self.other.pop().process()
        self.assertEqual(self.client.jobs[jid].state, 'failed')
        self.client.jobs[jid].move('other')
        FailJob().other(self.other.pop())
        self.assertEqual(self.client.jobs[jid].state, 'complete')
        
        jid = self.q.put(MissingJob, {})
        self.q.pop().process()
        self.assertEqual(self.client.jobs[jid].state, 'failed')

if __name__ == '__main__':
    unittest.main()
