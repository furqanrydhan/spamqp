#!/usr/bin/env python

import json
import os
import sys

import amqplib.client_0_8 as amqp

class _channel(amqp.channel.Channel):
    def __enter__(self):
        return self
    def __exit__(self, *args, **kwargs):
        self.close()

class _connection(amqp.Connection):
    def __init__(self, host, port):
        amqp.Connection.__init__(self, host=str(host) + ':' + str(port))
    def __enter__(self):
        return self
    def __exit__(self, *args, **kwargs):
        self.close()
    def channel(self):
        return _channel(self)



# Imperative approach
def declare(exchange_name, exchange_type, host='127.0.0.1', port=5672):
    with _connection(host, port) as connection:
        with connection.channel() as channel:
            channel.exchange_declare(
                exchange=exchange_name,
                type=exchange_type,
                durable=True,
                auto_delete=False
            )

def send(message, exchange_name, routing_key='#', host='127.0.0.1', port=5672):
    with _connection(host, port) as connection:
        with connection.channel() as channel:
            channel.basic_publish(
                amqp.Message(json.dumps(message)),
                exchange=exchange_name,
                routing_key=routing_key
            )
    
def bind(exchange_name, queue_name, routing_key='#', host='127.0.0.1', port=5672):
    with _connection(host, port) as connection:
        with connection.channel() as channel:
            channel.queue_declare(
                queue=queue_name,
                durable=True,
                exclusive=False,
                auto_delete=False
            )
            channel.queue_bind(
                queue=queue_name,
                exchange=exchange_name,
                routing_key=routing_key
            )
    
def receive(queue_name, host='127.0.0.1', port=5672):
    with _connection(host, port) as connection:
        with connection.channel() as channel:
            while True:
                message = channel.basic_get(queue=queue_name)
                if message is not None:
                    channel.basic_ack(message.delivery_tag)
                    return json.loads(message.body)

def listen(queue_name, callback, host='127.0.0.1', port=5672):
    with _connection(host, port) as connection:
        with connection.channel() as channel:
            def _callback(message):
                callback(json.loads(message.body))
                channel.basic_ack(message.delivery_tag)
            channel.basic_consume(queue=queue_name, callback=_callback)
            while True:
                channel.wait()



# Class-based approach
class _persistently_connected(object):
    def __init__(self, **kwargs):
        self._host = kwargs.get('host', '127.0.0.1')
        self._port = kwargs.get('port', 5672)
        self.__connection = None
        self.__channel = None
    def _connection(self):
        if self.__connection is None:
            self.__connection = _connection(self._host, self._port)
        return self.__connection
    def _channel(self):
        if self.__channel is None:
            self.__channel = self._connection().channel()
        return self.__channel
                
class producer(_persistently_connected):
    def __init__(self, **kwargs):
        _persistently_connected.__init__(self, **kwargs)
        # Without an exchange name, this won't work, so fail hard if it's
        # missing.
        self._exchange_name = kwargs.get('exchange_name', None)
        assert(self._exchange_name is not None)
        # If the exchange already exists, there is no need to declare the
        # exchange type.  However, for consistent operation in the startup
        # case as well as the steady-state running case, I find it useful
        # to be able to re-declare the exchange.
        self._exchange_type = kwargs.get('exchange_type', None)
        
        # If you've passed in sufficient information to identify the exchange,
        # delare it.  If the exchange already exists as you've described it,
        # this is a no-op.  If it does not, we'll throw an error, which you'll
        # probably want to investigate.
        if self._exchange_name is not None and self._exchange_type is not None:
            self._channel().exchange_declare(
                exchange=self._exchange_name,
                type=self._exchange_type,
                # In my experience these are the most useful sorts of exchanges.
                # If you really want to configure another kind, complain loudly
                # and I'll make these configurable.
                durable=True,
                auto_delete=False
            )
    def produce(self, message, routing_key='#'):
        self._channel().basic_publish(
            amqp.Message(json.dumps(message)),
            exchange=self._exchange_name,
            routing_key=routing_key
        )

class consumer(_persistently_connected):
    def __init__(self, **kwargs):
        _persistently_connected.__init__(self, **kwargs)
        # You must subscribe to a queue; this parameter is required.
        self._queue_name = kwargs.get('queue_name', None)
        assert(self._queue_name is not None)
        # A queue is filled by one or more exchanges which route messages
        # they receive to queues which subscribe to various routing keys.
        # 
        # The producer of messages doesn't need to know very much at all about
        # what happens to them after they are delivered to an exchange.  It is
        # sufficient that the producer assigns the appropriate routing key.
        # 
        # In theory the consumer could also be as ignorant as the producer.
        # If a consumer subscribes to a queue and the queue continues to
        # be filled with messages, does it really matter how they got there?
        # 
        # The more common usage, however, is that the consumer must ensure the
        # messages of interest are routed to the queue to which he subscribes.
        # Thus there are additional parameters for the consumer to cause the
        # bindings from exchange to queue to be created, and (to account for
        # extreme startup cases) to cause the exchange to be created in the
        # first place.  These parameters are optional, but if your consumer is
        # not responsible for binding exchanges and routing keys to deliver to
        # his queue... make sure you know who is.
        self._exchange_name = kwargs.get('exchange_name', None)
        self._exchange_type = kwargs.get('exchange_type', None)
        self._routing_keys = kwargs.get('routing_keys', None)
        if 'routing_keys' not in kwargs and self._exchange_type == 'direct':
            self._routing_keys = ['#']
        
        # Always declare the queue.  If the queue already exists this is a
        # no-op.  If it does not, you'll be glad we declared it.
        self._channel().queue_declare(
            queue=self._queue_name,
            # In my experience these are the most useful sorts of queues.  If
            # you really want to configure another kind, complain loudly and
            # I'll make these configurable.
            durable=True,
            exclusive=False,
            auto_delete=False
        )
        # If you've passed in sufficient information to identify the exchange,
        # delare it.  If the exchange already exists as you've described it,
        # this is a no-op.  If it does not, we'll throw an error, which you'll
        # probably want to investigate.
        if self._exchange_name is not None and self._exchange_type is not None:
            self._channel().exchange_declare(
                exchange=self._exchange_name,
                type=self._exchange_type,
                # In my experience these are the most useful sorts of exchanges.
                # If you really want to configure another kind, complain loudly
                # and I'll make these configurable.
                durable=True,
                auto_delete=False
            )
        # If you've passed in sufficient information to create the bindings,
        # declare them.  If the bindings already exist, this is a no-op.  If
        # they do not, you'll be glad we bound the exchange to your queue,
        # because otherwise you would not receive any messages.
        if self._exchange_name is not None and self._routing_keys is not None:
            assert(isinstance(self._routing_keys, list))
            for routing_key in self._routing_keys:
                self._channel().queue_bind(
                    queue=self._queue_name,
                    exchange=self._exchange_name,
                    routing_key=routing_key
                )
    # If you're creating an instance of this without subclassing, you can
    # call listen() and provide a callback method.
    def listen(self, callback):
        def _callback(message):
            callback(json.loads(message.body))
            self._channel().basic_ack(message.delivery_tag)
        self._channel().basic_consume(queue=self._queue_name, callback=_callback)
        while True:
            self._channel().wait()
    # If you're subclassing this, you can implement the process method, and
    # call loop().
    def loop(self):
        self.listen(callback=self.process)
    def process(self, message):
        raise NotImplemented