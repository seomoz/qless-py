qless [![Build Status](https://travis-ci.org/seomoz/qless-py.png)](https://travis-ci.org/seomoz/qless-py)
=====
Qless is a powerful `Redis`-based job queueing system inspired by
[resque](https://github.com/defunkt/resque#readme),
but built on a collection of Lua scripts, maintained in the
[qless-core](https://github.com/seomoz/qless-core) repo. Be sure to check the
changelog below.

Philosophy and Nomenclature
===========================
A `job` is a unit of work identified by a job id or `jid`. A `queue` can
contain several jobs that are scheduled to be run at a certain time, several
jobs that are waiting to run, and jobs that are currently running. A `worker`
is a process on a host, identified uniquely, that asks for jobs from the
queue, performs some process associated with that job, and then marks it as
complete. When it's completed, it can be put into another queue.

Jobs can only be in one queue at a time. That queue is whatever queue they
were last put in. So if a worker is working on a job, and you move it, the
worker's request to complete the job will be ignored.

A job can be `canceled`, which means it disappears into the ether, and we'll
never pay it any mind every again. A job can be `dropped`, which is when a
worker fails to heartbeat or complete the job in a timely fashion, or a job
can be `failed`, which is when a host recognizes some systematically
problematic state about the job. A worker should only fail a job if the error
is likely not a transient one; otherwise, that worker should just drop it and
let the system reclaim it.

Features
========

1. __Jobs don't get dropped on the floor__ -- Sometimes workers drop jobs.
	Qless automatically picks them back up and gives them to another worker
1. __Tagging / Tracking__ -- Some jobs are more interesting than others. Track
	those jobs to get updates on their progress. Tag jobs with meaningful
	identifiers to find them quickly in the UI.
1. __Job Dependencies__ -- One job might need to wait for another job to
	complete
1. __Stats__ -- `qless` automatically keeps statistics about how long jobs wait
	to be processed and how long they take to be processed. Currently, we keep
	track of the count, mean, standard deviation, and a histogram of these
	times.
1. __Job data is stored temporarily__ -- Job info sticks around for a 
	configurable amount of time so you can still look back on a job's history,
	data, etc.
1. __Priority__ -- Jobs with the same priority get popped in the order they
	were inserted; a higher priority means that it gets popped faster
1. __Retry logic__ -- Every job has a number of retries associated with it,
	which are renewed when it is put into a new queue or completed. If a job
	is repeatedly dropped, then it is presumed to be problematic, and is
	automatically failed.
1. __Web App__ -- With the advent of a Ruby client, there is a Sinatra-based
	web app that gives you control over certain operational issues
1. __Scheduled Work__ -- Until a job waits for a specified delay (defaults to
	0), jobs cannot be popped by workers
1. __Recurring Jobs__ -- Scheduling's all well and good, but we also support
	jobs that need to recur periodically.
1. __Notifications__ -- Tracked jobs emit events on pubsub channels as they get
	completed, failed, put, popped, etc. Use these events to get notified of
	progress on jobs you're interested in.

Interest piqued? Then read on!

Installation
============
Install from pip:

	pip install qless-py

Alternatively, install qless-py from source by checking it out from github,
and checking out the qless-core submodule:

```bash
git clone git://github.com/seomoz/qless-py.git
cd qless-py
# qless-core is a submodule
git submodule init
git submodule update
sudo python setup.py install
```

Business Time!
==============
You've read this far -- you probably want to write some code now and turn them
into jobs. Jobs are described essentially by two pieces of information -- a 
class` and `data`. The class should have static methods that know how to
process this type of job depending on the queue it's in. For those thrown for
a loop by this example, it's in reference to a
[South Park](http://en.wikipedia.org/wiki/Gnomes_%28South_Park%29#Plot)
episode where a group of enterprising gnomes set on world domination through
three steps: 1) collect underpants, 2) ? 3) profit!

```python
# In gnomes.py
class GnomesJob(object):
	# This would be invoked when a GnomesJob is popped off the 'underpants' queue
	@staticmethod
	def underpants(job):
		# 1) Collect Underpants
		...
		# Complete and advance to the next step, 'unknown'
		job.complete('unknown')
	
	@staticmethod
	def unknown(job):
		# 2) ?
		...
		# Complete and advance to the next step, 'profit'
		job.complete('profit')
	
	@staticmethod
	def profit(job):
		# 3) Profit
		...
		# Complete the job
		job.complete()
```

This makes it easy to describe how a `GnomesJob` might move through a pipeline,
first in the 'underpants' step, then 'unknown', and lastly 'profit.'
Alternatively, you can define a single method `process` that knows how to
complete the job, no matter what queue it was popped from. The above is just
meant as a convenience for pipelines:

```python
# Alternative gnomes.py
class GnomesJob(object):
	# This method would be invoked at every stage
	@staticmethod
	def process(job):
		if job['queue'] == 'underpants':
			...
			job.complete('underpants')
		elif job['queue'] == 'unknown':
			...
			job.complete('profit')
		elif job['queue'] == 'profit':
			...
			job.complete()
		else:
			job.fail('unknown-stage', 'What what?')
```

Jobs have user data associated with them that can be modified as it goes
through a pipeline. In general, you should make this data a dictionary, in
which case it's accessible through `__getitem__` and `__setitem__`. Otherwise,
it's accessible through `job.data`. For example, you might update the data...

```python
@staticmethod
def underpants(job):
	# Record how many underpants we collected
	job['collected'] = ...

@staticmethod
def unknown(job):
	# Make some decision based on how many we've collected.
	if job['collected'] ...:
		...
```

Great! With all this in place, let's put them in the queue so that they can
get run

```python
import qless
# Connecting to localhost on 6379
client = qless.Client()
# Connecting to a remote machine
client = qless.Client('redis://foo.bar.com:1234')
```

Now, reference a queue, and start putting your gnomes to work:

```python
queue = client.queues['underpants']

import gnomes
for i in range(1000):
	queue.put(gnomes.GnomesJob, {})
```

Alternatively, if the job class is not importable from where you're adding
jobs, you can use the full path of the job class as a string:

```python
...
for i in range(1000):
	queue.put('gnomes.GnomesJob', {})
```

__By way of a quick note__, it's important that your job class can be imported
-- you can't create a job class in an interactive prompt, for example. You can
_add_ jobs in an interactive prompt, but just can't define new job types.

Running
=======
All that remains is to have workers actually run these jobs. This distribution
comes with a script to help with this:

```bash
qless-py-worker -q underpants -q unknown -q profit
```

This script actually forks off several subprocesses that perform the work, and
the original process keeps tabs on them to ensure that they are all up and
running. In the future, the parent process might also perform other sanity
checks, but for the time being, it's just that the process is still alive. You
can specify the `host` and `port` you want to use for the qless server as well:

```bash
qless-py-worker --host foo.bar --port 1234 ...
```

In the absence of the `--workers` argument, qless will spawn as many workers
as there are cores on the machine. The interval specifies how often to poll 
in seconds) for work items. Future versions may have a mechanism to support
blocking pop.

```bash
qless-py-worker --workers 4 --interval 10
```

Because this works on a forked process model, it can be convenient to import
large modules _before_ subprocesses are forked. Specify these with `--import`:

```bash
qless-py-worker --import my.really.bigModule
```

Filesystem
----------
Previous versions of `qless-py` included a feature to have each worker process
run in its own sandbox directory. We've removed this feature because since
greenlets can't run in their own directory, the 'regular' and greenlet workers
behave differently.

In lieu of this behavior, each child process runs in its own sandboxed directory
and each job is given a `sandbox` attribute which is the name of a directory for
the sole use of that job. It's guaranteed to be clean by the time the job is
performed, and it cleaned up afterwards.

For example, if you invoke:

```bash
qless-py-worker --workers 4 --greenlets 5 --workdir foo
```

Then four child processes will be spawned using the directories:

```
foo/qless-py-workers/sandbox-{0,1,2,3}
```

The jobs run by the greenlets in the first process are given their own sandboxes
of the form:

```
foo/qless-py-workers/sandbox-0/greenlet-{0,1,2,3,4}
```

Gevent
------
Some jobs are I/O-bound, and might want to, say, make use of a greenlet pool.
If you have a class where you've, say, monkey-patched `socket`, you can ask
qless to create a pool of greenlets to run you job inside each process. To run
5 processes with 50 greenlets each:

```bash
qless-py-worker --workers 5 --greenlets 50
```

Signals
-------
With a worker running, you can send signals to child processes to:

- `USR1` - Get the current stack trace in that worker
- `USR2` - Enter a debugger in that worker

So, for example, if one of the worker child processes is `PID 1234`, then you
can invoke `kill -USR1 1234` to get the backtrace in the logs (and console
output).

Resuming Jobs
-------------
This is an __experimental__ feature, but you can start workers `--resume` flag
to have the worker begin its processing with the jobs it left off with. For
instance, during deployments, it's common to restart the worker processes, and
the `--resume` flag has the worker first perform a check with `qless` to see
which jobs it had last been running (and still has locks for).

This flag should be used with some caution. In particular, if two workers are
running with the same worker name, then this should not be used. The reason is
that through the `qless` interface, it's impossible to differentiate the two,
and currently-running jobs may be confused with jobs that were simply dropped
when the worker was stopped.

Debugging / Developing
======================
Whenever a job is processed, it checks to see if the file in which your job is
defined has been updated since its last import. If it has, it automatically
reimports it. We think of this as a feature.

With this in mind, when I start a new project and want to make use of qless, I
first start up the web app locally (see
[`qless`](http://github.com/seomoz/qless) for more), take a first pass, and
enqueue a single job while the worker is running:

	# Supposing that I have /my/awesome/project/awesomeproject.py
	# In one terminal...
	qless-py-worker --path /my/awesome/project --queue foo --workers 1 --interval 10 --verbose
	
	# In another terminal...
	>>> import qless
	>>> import awesomeproject
	>>> qless.Client().queues['foo'].put(awesomeproject.Job, {'key': 'value'))

From there, I watch the output on the worker, adjust my job class, save it,
watch again, etc., but __without restarting the worker__ -- in general it
shouldn't be necessary to restart the worker.

Internals and Additional Features
=================================
While in many cases the above is sufficient, there are also many cases where
you may need something more. Hopefully after this section many of your
questions will be answered.

Priority
--------
Jobs can optionally have priority associated with them. Jobs of equal priority
are popped in the order in which they were put in a queue. The higher the
priority, the sooner it will be processed. If, for example, you get a new job
to collect some really valuable underpants:

```python
queue.put(qless.gnomes.GnomesJob, {'address': '123 Brief St.'}, priority = 10)
```

You can also adjust a job's priority while it's waiting:

```python
job = client.jobs['83da4d32a0a811e1933012313b062cf1']
job.priority = 25
```

Scheduled Jobs
--------------
Jobs can also be scheduled for the future with a delay (in seconds). If for
example, you just learned of an underpants heist opportunity, but you have to
wait until later:

```python
queue.put(qless.gnomes.GnomesJob, {}, delay=3600)
```

It's worth noting that it's not guaranteed that this job will run at that time.
It merely means that this job will only be considered valid after the delay
has passed, at which point it will be subject to the normal constraints. If
you want it to be processed very soon after the delay expires, you could also
boost its priority:

```
queue.put(qless.gnomes.GnomesJob, {}, delay=3600, priority=100)
```

Recurring Jobs
--------------
Whether it's nightly maintainence, or weekly customer updates, you can have a
job of a certain configuration set to recur. Recurring jobs still support
priority, and tagging, and are attached to a queue. Let's say, for example, I
need some global maintenance to run, and I don't care what machine runs it, so
long as someone does:

```python
client.queues['maintenance'].recur(myJob, {'tasks': ['sweep', 'mop', 'scrub']}, interval=60 * 60 * 24)
```

That will spawn a job right now, but it's possible you'd like to have it recur,
but maybe the first job should wait a little bit:

```python
client.queues['maintenance'].recur(..., interval=86400, offset=3600)
```

You can always update the tags, priority and even the interval of a recurring job:

```python
job = client.jobs['83da4d32a0a811e1933012313b062cf1']
job.priority = 20
job.tag('foo', 'bar')
job.untag('hello')
job.interval = 7200
```

These attributes aren't attached to the recurring jobs, per se, but it's used
as the template for the job that it creates. In the case where more than one
interval passes before a worker tries to pop the job, __more than one job is
created__. The thinking is that while it's
completely client-managed, the state should not be dependent on how often
workers are trying to pop jobs.

```python
# Recur every minute
queue.recur(..., {'lots': 'of jobs'}, 60)
# Wait 5 minutes
len(queue.pop(10))
# => 5 jobs got popped
```

Configuration Options
=====================
You can get and set global (read: in the context of the same Redis instance)
configuration to change the behavior for heartbeating, and so forth. There
aren't a tremendous number of configuration options, but an important one is
how long job data is kept around. Job data is expired after it has been
completed for `jobs-history` seconds, but is limited to the last
`jobs-history-count` completed jobs. These default to 50k jobs, and 30 days,
but depending on volume, your needs may change. To only keep the last 500 jobs
for up to 7 days:

```python
client.config['jobs-history'] = 7 * 86400
client.config['jobs-history-count'] = 500
```

Tagging / Tracking
------------------
In qless, 'tracking' means flagging a job as important. Tracked jobs have a
tab reserved for them in the web interface, and they also emit subscribable
events as they make progress (more on that below). You can flag a job from the
web interface, or the corresponding code:

```python
client.jobs['b1882e009a3d11e192d0b174d751779d'].track()
```

Jobs can be tagged with strings which are indexed for quick searches. For
example, jobs might be associated with customer accounts, or some other key
that makes sense for your project.

```python
queue.put(qless.gnomes.GnomesJob, {'tags': 'aplenty'}, tags=['12345', 'foo', 'bar'])
```

This makes them searchable in the web interface, or from code:

```python
jids = client.jobs.tagged('foo')
```

You can add or remove tags at will, too:

```python
job = client.jobs['b1882e009a3d11e192d0b174d751779d']
job.tag('howdy', 'hello')
job.untag('foo', 'bar')
```

Job Dependencies
----------------
Jobs can be made dependent on the completion of another job. For example, if
you need to buy eggs, and buy a pan before making an omelete, you could say:

```python
eggs_jid = client.queues['buy_eggs'].put(myJob, {'count': 12})
pan_jid  = client.queues['buy_pan' ].put(myJob, {'coating': 'non-stick'})
client.queues['omelete'].put(myJob, {'toppings': ['onions', 'ham']}, depends=[eggs_jid, pan_jid])
```

That way, the job to make the omelete can't be performed until the pan and eggs
purchases have been completed.

Notifications
-------------
Tracked jobs emit events on specific pubsub channels as things happen to them.
Whether it's getting popped off of a queue, completed by a worker, etc. The
jist of it goes like this, though:

```python
def callback(evt, jid):
	print '%s => %s' % (jid, evt)

from functools import partial
for evt in ['canceled', 'completed', 'failed', 'popped', 'put', 'stalled', 'track', 'untrack']:
	client.events.on(evt, partial(callback, evt))
client.events.listen()
```

If you're interested in, say, getting growl or campfire notifications, you
should check out the `qless-growl` and `qless-campfire` ruby gems.

Retries
-------
Workers sometimes die. That's an unfortunate reality of life. We try to
mitigate the effects of this by insisting that workers heartbeat their jobs to
ensure that they do not get dropped. That said, qless will automatically
requeue jobs that do get 'stalled' up to the provided number of retries
(default is 5). Since underpants profit can sometimes go awry, maybe you want
to retry a particular heist several times:

```python
queue.put(qless.gnomes.GnomesJob, {}, retries=10)
```

Pop
---
A client pops one or more jobs from a queue:

```python
# Get a single job
job = queue.pop()
# Get 20 jobs
jobs = queue.pop(20)
```

Heartbeating
------------
Each job object has a notion of when you must either check in with a heartbeat 
or turn it in as completed. You can get the absolute time until it expires, or
how long you have left:

```python
# When I have to heartbeat / complete it by (seconds since epoch)
job.expires_at
# How long until it expires
job.ttl
```

If your lease on the job will expire before you have a chance to complete it,
then you should heartbeat it to make sure that no other worker gets access to
it. Or, if you are done, you should complete it so that the job can move on:

```python
# I call stay-offsies!
job.heartbeat()
# I'm done!
job.complete()
# I'm done with this step, but need to go into another queue
job.complete('anotherQueue')
```

Stats
-----
One of the selling points of qless is that it keeps stats for you about your 
underpants hijinks. It tracks the average wait time, number of jobs that have
waited in a queue, failures, retries, and average running time. It also keeps
histograms for the number of jobs that have waited _x_ time, and the number 
that took _x_ time to run.

Frankly, these are best viewed using the web app.

Lua
---
Qless is a set of client language bindings, but the majority of the work is
done in a collection of Lua scripts that comprise the
[core](https://github.com/seomoz/qless-core) functionality. These scripts run
on the Redis 2.6+ server atomically and allow for portability with the same
functionality guarantees. Consult the documentation for `qless-core` to learn
more about its internals.

Web App
-------
`Qless` also comes with a web app for administrative tasks, like keeping tabs
on the progress of jobs, tracking specific jobs, retrying failed jobs, etc.
It's available in the [`qless`](https://github.com/seomoz/qless) library as a
mountable [`Sinatra`](http://www.sinatrarb.com/) app. The web app is language
agnostic and was one of the major desires out of this project, so you should
consider using it even if you're not planning on using the Ruby client.

Changelog
=========
Things that have changed over time.

v0.10.0
-------
The major change was the switch to `unified` qless. This change is
semi-incompatibile. In particular, it changes the job history format but the new
version knows how to convert the old format forward. Upgrades to your workers
should be made from the end of pipelines towards the start. It will also be
necessary to upgrade your `qless-web` install if you're using it.

- Preempts workers running jobs for which they've lost their lock
- Improved coverage (98%, up from 71%), all of which was worker code
- Debugging signals
- Resumable workers
- Redis URL interface. When specifying a qless client, the default is still to
	point to `localhost:6379`, but rather than specify `host` and `port`, you
	should provide a single `host` argument of a Redis URL format. For example,
	`redis://user:auth@host:port/db`. Many of these paremeters are optional, but
	it seems to be the convention recently.

Upgrading to qless-py 0.10.0
============================
Some notes, instructions and potential road blocks to the upgrade. This version
has much better coverage, and a few added features, including stalled job
preemption, pauseable queues, unified sandboxing and the ability to use the
cleaner web interface.

Road Blocks
===========
Before we talk about how to install the updated client, here are a couple
potential road blocks that will need to be addressed before you can make the
switch.

Sandboxes
---------
If you were using sandboxes (if using the non-greenlet client) and relying on
using the current working directory as the sandbox, that interface has been
done away with. The replacement is that each job comes with a `sandbox`
attribute which is guaranteed to be a directory that exists and empty at the
start of the job, and which is cleaned up after the job. It's a great place for
temporary files. __This only applies if you are running a qless-worker, and not
if you are using the qless client directly to work on jobs.__

The directories are made up of subdirectories under the directory provided as
`--workdir`, defaulting to the current directory.

Client Rename
-------------
If you are using the `qless` client directly, all instances of `qless.client`
will have to change to `qless.Client`. It was an unfortunate mistake that it was
ever named `client` to begin with, but hopefully this change won't be painful.

Redis Server Spec
-----------------
There was a feature request to be able to provide redis auth credentials, and
rather than support any new attributes to the redis client that might come
along, we'll now use a [redis url](http://redis-py.readthedocs.org/en/latest/#redis.StrictRedis.from_url).

For example:

```python
# Instead of this
client = qless.Client(host='foo', port=6380)
# Now it's this
client = qless.Client(url='redis://foo:6380')
```

This allows users to provide auth, select a database, etc. Remember to change
this in __worker invocations__ and __config files__.


Installation
============
With an existing copy of `qless-py` checked out

```bash
# Get the most recent version
git fetch
git checkout v0.10.0

# Checkout, update and build the submodule
git submodule init
git submodule update
make -C qless/qless-code

# Install dependencies and then qless
sudo pip install -r requirements.txt
sudo python setup.py install
```
