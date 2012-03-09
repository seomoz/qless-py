qless-py
========

Python bindings for [`qless`](https://github.com/seomoz/qless-core).

Installation
============

For qless, you'll need redis (and optionally hireds). Then qless can be installed
from this repo with:

	# Install hiredis, redis
	sudo pip install hiredis redis
	sudo setup.py install

Usage
=====

First things first, import qless and make a client the same way you'd make a redis
connection:

	import qless
	
	# Connecting to localhost on 6379
	client = qless.client()
	# Connecting to a remote machine
	client = qless.client(host='foo.bar.com', port=1234)

From there, you can access the queues to get a job:

	# Get a reference to a queue
	queue = client.queue('testing')
	# Get a work item
	job = queue.pop()
	# Get 20 jobs
	jobs = queue.pop(20)

A job has certain pieces of metadata associated with it, like its `id`, `state`, `priority`,
`worker`, which is your worker's identifier if you just got it as a piece of work. It also
has a useful attribute, `data`, which contains user-specified data associated with a job. It
is a dictionary, and can be accessed through `__getitem__` and `__setitem__`. When you heartbeat
or complete a job, any changes you've made to the job will update the central notion of the
job. For example,

	# I know this job is supposed to have some data 'widget'
	# I can get it like:
	job['widget']
	# Or
	job.data['widget']
	# And I can update it
	job['widget'] = 'foo'

Each job object has a notion of when you must either check in with a heartbeat or
turn it in as completed. You can get the absolute time until it expires, or how 
long you have left:

	# When I have to heartbeat / complete it by (seconds since epoch)
	job.expires
	# How long until it expires
	job.remaining()

If your lease on the job will expire before you have a chance to complete it, then
you should heartbeat it to make sure that no other worker gets access to it. Or, if
you are done, you should complete it so that the job can move on:

	# I call stay-offsies!
	queue.heartbeat(job)
	# I'm done!
	queue.complete(job)

Configuration
=============

Stats
=====














