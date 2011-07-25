#!/usr/bin/env python

import socket
import traceback

import _amqp

class consumer(_amqp._amqp):
    _queue_name = None
    _routing_keys = None
    def __init__(self, *args, **kwargs):
        if self._queue_name is None:
            if 'queue_name' in kwargs:
                self._queue_name = kwargs['queue_name']
                del kwargs['queue_name']
        if self._routing_keys is None:
            if 'routing_keys' in kwargs:
                self._routing_keys = kwargs['routing_keys']
                del kwargs['routing_keys']
            else:
                self._routing_keys = ['#']
        _amqp._amqp.__init__(self, *args, **kwargs)
    def initialize(self):
        _amqp._amqp.initialize(self)
        with self.channel() as channel:
            channel.queue_declare(queue=self._queue_name, durable=True, exclusive=False, auto_delete=False)
            for routing_key in self._routing_keys:
                channel.queue_bind(queue=self._queue_name, exchange='_'.join([self._exchange_name, self._exchange_type]), routing_key=routing_key)
    def process(self, message):
        print message
        return True
    def loop(self):
        socket_error_reported = False
        def consumer(message):
            try:
                if self.process(message.body):
                    # Message succeeded.
                    channel.basic_ack(message.delivery_tag)
                else:
                    # Reject this message.  Allow the server to try again in case it was a transient condition.
                    # TODO retries seem to be immediately redelivered to me, abandon that idea for now.
                    channel.basic_reject(message.delivery_tag, False)#True)
            except KeyboardInterrupt:
                raise
            except:
                print 'Unhandled consumer error, rejecting message'
                channel.basic_reject(message.delivery_tag, False)
        while True:
            try:
                if not self._initialized:
                    self.initialize()
                    socket_error_reported = False
                with self.channel() as channel:
                    try:
                        channel.basic_consume(queue=self._queue_name, callback=consumer, consumer_tag=str(id(self)))
                        while True:
                            channel.wait()
                    except KeyboardInterrupt:
                        channel.basic_cancel(str(id(self)))
                        raise
                    except:
                        print traceback.format_exc()
                        raise
            except socket.error:
                if not socket_error_reported:
                    print 'Socket error; no AMQP server at ' + str(self._settings.get('amqp', {}).get('host', None)) + '?'
                    self._initialized = False
                    socket_error_reported = True
                    
if __name__ == '__main__':
    import sys
    queue = sys.argv[1] if len(sys.argv) > 1 else 'test'
    import config
    settings = config.config()
    c = consumer(queue_name=queue, exchange_name=queue, exchange_type='direct', settings=settings)
    c.loop()