'''A class that listens to pubsub channels and can unlisten'''

import logging
import threading
import contextlib

# Our logger
logger = logging.getLogger('qless')


class Listener(object):
    '''A class that listens to pubsub channels and can unlisten'''
    def __init__(self, redis, channels):
        self._pubsub = redis.pubsub()
        self._channels = channels

    def listen(self):
        '''Listen for events as they come in'''
        try:
            self._pubsub.subscribe(self._channels)
            for message in self._pubsub.listen():
                if message['type'] == 'message':
                    yield message
        finally:
            self._channels = []

    def unlisten(self):
        '''Stop listening for events'''
        self._pubsub.unsubscribe(self._channels)

    @contextlib.contextmanager
    def thread(self):
        '''Run in a thread'''
        thread = threading.Thread(target=self.listen)
        thread.start()
        try:
            yield self
        finally:
            self.unlisten()
            thread.join()


class Events(Listener):
    '''A class for handling qless events'''
    namespace = 'ql:'
    events = (
        'canceled', 'completed', 'failed', 'popped',
        'stalled', 'put', 'track', 'untrack'
    )

    def __init__(self, redis):
        Listener.__init__(
            self, redis, [self.namespace + event for event in self.events])
        self._callbacks = dict((k, None) for k in (self.events))

    def listen(self):
        '''Listen for events'''
        for message in Listener.listen(self):
            logger.debug('Message: %s', message)
            # Strip off the 'namespace' from the channel
            channel = message['channel'][len(self.namespace):]
            func = self._callbacks.get(channel)
            if func:
                func(message['data'])

    def on(self, evt, func):
        '''Set a callback handler for a pubsub event'''
        if evt not in self._callbacks:
            raise NotImplementedError('callback "%s"' % evt)
        else:
            self._callbacks[evt] = func

    def off(self, evt):
        '''Deactivate the callback for a pubsub event'''
        return self._callbacks.pop(evt, None)
