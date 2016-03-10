# Python3 compatibility

import itertools
import sys

if sys.version_info[0] >= 3:
    basestring = (bytes, str)
    izip_longest = itertools.zip_longest
    next = next
else:
    basestring = basestring
    izip_longest = itertools.izip_longest
    def next(generator):
        return generator.next()
