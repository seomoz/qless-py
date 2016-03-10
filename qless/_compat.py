# Python3 compatibility

import itertools
import sys

if sys.version_info[0] >= 3:
    basestring = (bytes, str)
    izip_longest = itertools.zip_longest
else:
    basestring = basestring
    izip_longest = itertools.izip_longest
