#! /usr/bin/env python

'''Some exception classes'''


class QlessException(Exception):
    '''Any and all qless exceptions'''
    pass


class LostLockException(QlessException):
    '''Lost lock on a job'''
    pass
