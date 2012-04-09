from shovel import task

@task
def demodata(host='localhost', port=6379):
    '''Fill the qless instance with demo data'''
    import qless
    import random
    from qless import demo
    # Alright, first, we're going to add a bunch of jobs waiting to go through the pipeline
    # Alright, let's make sure that we can do this
    client = qless.client(host=host, port=int(port))
    crawl = client.queue('crawl')
    domains = [random.choice(['github.com', 'seomoz.org', 'foo.bar', 'wikipedia.org', 'mtv.com']) for i in range(1000)]
    jids = [crawl.put(demo.CrawlJob({
        'status_callback_url': 'http://seomoz.org/something?12345',
        'stages': {
            'seed': {
                'url': domain,
                'crawl_across_subdomains': False,
                'max_time': 7,
                'max_depth': 10000
            }
        }
    }, tags=[domain])) for domain in domains]
