#! /usr/bin/env python

from setuptools import setup

setup(
    name                 = 'qless-py',
    version              = '0.11.0-dev',
    description          = 'Redis-based Queue Management',
    long_description     = '''
Redis-based queue management, with heartbeating, job tracking,
stats, notifications, and a whole lot more.''',
    url                  = 'http://github.com/seomoz/qless-py',
    author               = 'Dan Lecocq',
    author_email         = 'dan@moz.com',
    license              = "MIT License",
    keywords             = 'redis, qless, job',
    packages             = [
        'qless',
        'qless.workers'
    ],
    package_dir          = {
        'qless': 'qless',
        'qless.workers': 'qless/workers'
    },
    package_data         = {
        'qless': [
            'qless-core/*.lua'
        ]
    },
    include_package_data = True,
    scripts              = [
        'bin/qless-py-worker'
    ],
    extras_require       = {
        'ps': [
            'setproctitle'
        ]
    },
    install_requires     = [
        'argparse',
        'decorator',
        'hiredis',
        'redis',
        'psutil',
        'six',
        'simplejson'
    ],
    tests_requires       = [
        'coverage',
        'mock',
        'nose',
        'setuptools>=17.1'
    ],
    classifiers          = [
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Intended Audience :: Developers',
        'Operating System :: OS Independent'
    ]
)
