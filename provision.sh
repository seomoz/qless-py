#! /usr/bin/env bash

set -e

# Some dependencies
sudo apt-get update
sudo apt-get install -y libhiredis-dev libevent-dev python-pip python-dev

# Install redis in support of qless
sudo apt-get install -y redis-server

# Install dependencies and the thing itself
(
    cd /vagrant/
    sudo pip install -r requirements.txt
    sudo python setup.py install
)
