#! /usr/bin/env python

try:
    from setuptools import setup
    extra = {
        'install_requires' : [
            'argparse', 'hiredis', 'redis', 'psutil', 'simplejson']
    }
except ImportError:
    from distutils.core import setup
    extra = {
        'dependencies' : [
            'argparse', 'hiredis', 'redis', 'psutil', 'simplejson']
    }

setup(
    name                 = 'qless-py',
    version              = '0.9.3',
    description          = 'Redis-based Queue Management',
    long_description     = '''
Redis-based queue management, with heartbeating, job tracking,
stats, notifications, and a whole lot more.
    ''',
    url                  = 'http://github.com/seomoz/qless-py',
    author               = 'Dan Lecocq',
    author_email         = 'dan@seomoz.org',
    license              = "MIT License",
    keywords             = 'redis, qless, job',
    packages             = ['qless'],
    package_dir          = {'qless': 'qless'},
    package_data         = {'qless': ['qless-core/*.lua']},
    scripts              = ['bin/qless-py-worker'],
    include_package_data = True,
    extras_require       = {
        'ps': ['setproctitle']
    },
    classifiers          = [
        'License :: OSI Approved :: MIT License',
    	'Programming Language :: Python',
    	'Intended Audience :: Developers',
    	'Operating System :: OS Independent'
    ],
    **extra
)
