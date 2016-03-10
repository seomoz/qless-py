# Python3 compatibility

import sys

if sys.version_info[0] >= 3:
    basestring = (bytes, str)
else:
    basestring = basestring
