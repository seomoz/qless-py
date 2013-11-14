'''Some utility functions'''


def import_class(klass):
    '''Import the named class and return that class'''
    mod = __import__(klass.rpartition('.')[0])
    for segment in klass.split('.')[1:-1]:
        mod = getattr(mod, segment)
    return getattr(mod, klass.rpartition('.')[2])
