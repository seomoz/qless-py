# Python3 compatibility

import itertools
import sys

if sys.version_info[0] >= 3:
    basestring = (bytes, str)
    izip_longest = itertools.zip_longest
    next = next

    if sys.version_info[1] >= 4:
        import importlib
        reload = importlib.reload
    else:
        import imp
        reload = imp.reload
else:
    basestring = basestring
    izip_longest = itertools.izip_longest
    reload = reload

    def next(generator):
        return generator.next()
