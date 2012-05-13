qless-py
========

Python bindings for [`qless`](https://github.com/seomoz/qless). Qless is a work
queue, based on Redis, and inspired by Resque, but with several key differences:

1. __Jobs can't get dropped__ -- jobs have to be checked back in as completed, and 
	long jobs can heartbeat, to ensure that they don't get quietly dropped on
	the floor by a worker
1. __Stats__ -- qless automatically keeps summary statistics about how long jobs wait,
	how long they take to run, etc.
1. __Scheduling__ -- qless supports scheduling jobs right out of the box
1. __Dependendies__ -- jobs can wait for another job(s) to complete before being 
	worked on
1. __Recurring Jobs__ -- jobs can be set to recur periodically
1. __History__ -- each job knows everything that's happend to it. From when it was
	first enqueued, to when it was popped, failed, and completed
1. __Priority__ -- jobs aren't restricted to priorities of 'high', 'medium' and 'low',
	but any integer. Jobs with the same priority are popped off in the order
	they were inserted
1. __Tagging and Tracking__ -- jobs can be tagged in a searchable way, and flagged for
	tracking, making it easy to keep tabs on important jobs (like jobs mentioned
	in bug reports, for example).

With all these differences, it's like Resque in that it comes bundled with a web 
app (available in the `qless` gem), and is high-performance.

Interest piqued? Then read on!

Installation
============

You can install qless-py from source by checking it out from github, and checking out
the qless-core submodule:

	git clone git://github.com/seomoz/qless-py.git
	cd qless-py
	# qless-core is a submodule
	git submodule init
	git submodule update
	sudo python setup.py install

Business Time!
==============
You've read this far -- you probably want to write some code now and turn them into jobs.
Jobs are described essentially by two pieces of information -- a `class` and `data`.
The class should have static methods that know how to process this type of job depending
on the queue it's in. For those thrown for a loop by this example, it's in refrence to
a [South Park](http://en.wikipedia.org/wiki/Gnomes_(South_Park\)) episode with a group of
enterprising gnomes set on world domination through three steps: 1) collect underpants, 
2) ? 3) profit!

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

This makes it easy to describe how a `GnomesJob` might move through a pipeline,
first in the 'underpants' step, then 'unknown', and lastly 'profit.' Alternatively,
you can define a single method `process` that knows how to complete the job, no matter 
what queue it was popped from. The above is just meant as a convenience for pipelines:

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

Jobs have user data associated with them that can be modified as it goes through a
pipeline. In general, you should make this data a dictionary, in which case it's 
accessible through `__getitem__` and `__setitem__`. Otherwise, it's accessible through
`job.data`. For example, you might update the data...

	@staticmethod
	def underpants(job):
		# Record how many underpants we collected
		job['collected'] = ...
	
	@staticmethod
	def unknown(job):
		# Make some decision based on how many we've collected.
		if job['collected'] ...:
			...

Great! With all this in place, let's put them in the queue so that they can get run

	import qless
	# Connecting to localhost on 6379
	client = qless.client()
	# Connecting to a remote machine
	client = qless.client(host='foo.bar.com', port=1234)

Now, reference a queue, and start putting your gnomes to work:

	queue = client.queue('underpants')
	
	import gnomes
	for i in range(1000):
		queue.put(gnomes.GnomesJob, {})

__By way of a quick note__, it's important that your job class can be imported -- you can't
create a job class in an interactive prompt, for example. You can _add_ jobs in an interactive
prompt, but just can't define new job types.

Running
=======
All that remains is to have workers actually run these jobs. This distribution comes with a
script to help with this:

	qless-py-worker --queue underpants --queue unknown --queue profit

This script actually forks off several subprocesses that perform the work, and the original
process keeps tabs on them to ensure that they are all up and running. In the future, the
parent process might also perform other sanity checks, but for the time being, it's just
that the process is still alive. You can specify the `host` and `port` you want to use for
the qless server as well:

	qless-py-worker --host foo.bar --port 1234 ...

In the absence of the `--workers` argument, qless will spawn as many workers as there are 
cores on the machine. The interval specifies how often to poll (in seconds) for work items.
Future versions may have a mechanism to support blocking pop.

	qless-py-worker --workers 4 --interval 10

Because this works on a forked process model, it can be convenient to import large modules
_before_ subprocesses are forked. Specify these with `--import`:

	qless-py-worker --import my.really.bigModule

Filesystem
----------
Each worker runs in a sandbox directory that:

1. Clobbers any files in it left _after_ running a job
1. Clobbers any files in it _before_ running a job

The worker runs in the context of that directory, so when you create files like this,

	with file('foo.txt') as f:
		...

they get created in the worker's sandbox. This can be useful for storing temporary files,
but it also means that any files that need to persist should either be put somewhere 
specific, or uploaded somewhere, etc. These sandboxes have the form
`<workdir>/qless-py-workers/sandbox-<k>/`, so when running the worker, you can specify a
particular working directory as the base,

	qless-py-worker --workdir /home/foo/awesome-project

which would yield sandboxes `/home/foo/awesome-project/qless-py-workers/sandbox-<k>`.

Gevent
------
Some jobs are I/O-bound, and might want to, say, make use of a greenlet pool. If you have
a class where you've, say, monkey-patched `socket`, you can ask qless to create a pool of
greenlets to run you job inside each process. To run 5 processes with 50 greenlets each:

	qless-py-worker --workers 5 --greenlets 50

Debugging / Developing
======================
Whenever a job is processed, it checks to see if the file in which your job is defined has
been updated since its last import. If it has, it automatically reimports it. We think of
this as a feature.

With this in mind, when I start a new project and want to make use of qless, I first start
up the web app locally (see [`qless`](http://github.com/seomoz/qless) for more), take a
first pass, and enqueue a single job while the worker is running:

	# Supposing that I have /my/awesome/project/awesomeproject.py
	# In one terminal...
	qless-py-worker --path /my/awesome/project --queue foo --workers 1 --interval 10 --verbose
	
	# In another terminal...
	>>> import qless
	>>> import awesomeproject
	>>> qless.client().queue('foo').put(awesomeproject.Job, {'key': 'value'))

From there, I watch the output on the worker, adjust my job class, save it, watch again, etc.,
but __without restarting the worker__ -- in general it shouldn't be necessary to restart the
worker.

Internals and Additional Features
=================================
While in many cases the above is sufficient, there are also many cases where you may need 
something more. Hopefully after this section many of your questions will be answered.

Priority
--------
Jobs can optionally have priority associated with them. Jobs of equal priority are popped
in the order in which they were put in a queue. The lower the priority, the sooner it will
be processed (it's sort of like `nice`ness). If, for example, you get a new job to collect
some really valuable underpants, then:

	queue.put(qless.gnomes.GnomesJob, {'address': '123 Brief St.'}, priority = -100)

Tags
----
Jobs can have string tags associated with them. Currently, they're justs a piece of metadata
that's associated with each job, but in the future, these will likely be indexed for quick
access.

	queue.put(qless.gnomes.GnomesJob, {}, tags=['tidy', 'white', 'briefs'])

Delay
-----
Jobs can also be scheduled for the future with a delay (in seconds). If for example, you just
learned of an underpants heist opportunity, but you have to wait until later:

	queue.put(qless.gnomes.GnomesJob, {}, delay=3600)

It's worth noting that it's not guaranteed that this job will run at that time. It merely means
that this job will only be considered valid after the delay has passed, at which point it will
be subject to the normal constraints. If you want it to be processed very soon after the delay
expires, you could also boost its priority:

	queue.put(qless.gnomes.GnomesJob, {}, delay=3600, priority=-1000)
	
Job Dependencies
----------------
Jobs can be made dependent on the completion of another job. For example, if you
need to buy eggs, and buy a pan before making an omelete, you could say:

	eggs_jid = client.queues['buy_eggs'].put(myJob, {'count': 12})
	pan_jid  = client.queues['buy_pan' ].put(myJob, {'coating': 'non-stick'})
	client.queues['omelete'].put(myJob, {'toppings': ['onions', 'ham']}, depends=[eggs_jid, pan_jid])

That way, the job to make the omelete can't be performed until the pan and eggs
purchases have been completed.

Recurring Jobs
--------------
Whether it's nightly maintainence, or weekly customer updates, you can have a
job of a certain configuration set to recur. Recurring jobs still support priority,
and tagging, and are attached to a queue. Let's say, for example, I need some
global maintenance to run, and I don't care what machine runs it, so long as
someone does:

	client.queues['maintenance'].recur(myJob, {'tasks': ['sweep', 'mop', 'scrub']}, interval=60 * 60 * 24)

That will spawn a job right now, but it's possible you'd like to have it recur,
but maybe the first job should wait a little bit:

	client.queues['maintenance'].recur(..., interval=86400, offset=3600)

Retries
-------
Workers sometimes die. That's an unfortunate reality of life. We try to mitigate the effects of
this by insisting that workers heartbeat their jobs to ensure that they do not get dropped. That
said, qless will automatically requeue jobs that do get 'stalled' up to the provided number of
retries (default is 5). Since underpants profit can sometimes go awry, maybe you want to retry
a particular heist several times:

	queue.put(qless.gnomes.GnomesJob, {}, retries=10)

Pop
---
A client pops one or more jobs from a queue:

	# Get a single job
	job = queue.pop()
	# Get 20 jobs
	jobs = queue.pop(20)

Heartbeating
------------
Each job object has a notion of when you must either check in with a heartbeat or
turn it in as completed. You can get the absolute time until it expires, or how 
long you have left:

	# When I have to heartbeat / complete it by (seconds since epoch)
	job.expires
	# How long until it expires
	job.ttl()

If your lease on the job will expire before you have a chance to complete it, then
you should heartbeat it to make sure that no other worker gets access to it. Or, if
you are done, you should complete it so that the job can move on:

	# I call stay-offsies!
	job.heartbeat()
	# I'm done!
	job.complete()
	# I'm done with this step, but need to go into another queue
	job.complete('anotherQueue')

Stats
-----
One of the selling points of qless is that it keeps stats for you about your 
underpants hijinks. It tracks the average wait time, number of jobs that have
waited in a queue, failures, retries, and average running time. It also keeps
histograms for the number of jobs that have waited _x_ time, and the number 
that took _x_ time to run.

Frankly, these are best viewed using the web app.

Web App
=======

`Qless` also comes with a web app for administrative tasks, like keeping tabs on the 
progress of jobs, tracking specific jobs, retrying failed jobs, etc. It's available
in the [`qless`](https://github.com/seomoz/qless) library as a mountable
[`Sinatra`](http://www.sinatrarb.com/) app. The web app is language agnostic and was
one of the major desires out of this project, so you should consider using it even 
if you're not planning on using the Ruby client.

Internals
=========

Lua
---

Qless is a set of client language bindings, but the majority of the work is done in 
a collection of Lua scripts that comprise the [core](https://github.com/seomoz/qless-core)
functionality. These scripts run on the Redis 2.6+ server atomically and allow for
portability with the same functionality guarantees. Consult the documentation for 
`qless-core` to learn more about its internals.

Concepts and Philosphy
======================

Jobs are units of work that can be placed in queues. Jobs keep track of the history of
put / pop / fail events, as well as workers that have worked on a job. A job can appear
in only one queue at a time, and have a type and a JSON blob of user data associated
with it.

Workers can pop a job, and they get an exclusive lock on that job for a limited time.
This lock can be renewed by heartbeating the job to assure qless that the worker has
not disappeared and is indeed still working on it. The maximum allowable time between
heartbeats is configurable.

Configuration
=============

Qless maintains global configuration for certain pieces of data:

1. `heartbeat` (60) | default heartbeat (seconds) for queues
1. `heartbeat-<queue-name>` | heartbeat (seconds) for a specific queue
1. `stats-history` (30) | number of days for which to store summary stats
1. `histogram-history` (7) | The number of days to store histogram data
1. `jobs-history-count` (50k) --
	How many jobs to keep data for after they're completed
1. `jobs-history` (7 * 24 * 60 * 60) --
	How many seconds to keep jobs after they're completed
