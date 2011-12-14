#!/usr/bin/env python

__version_info__ = (0, 1, 4)
__version__ = '.'.join([str(i) for i in __version_info__])
version = __version__



import json
import os
import pika
import random
import socket
import sys



DEFAULT_HOST = '127.0.0.1'
DEFAULT_PORT = 5672



class AMQPException(Exception):
    def __init__(self, message=None):
        self.message = message
    def __str__(self):
        return self.message or 'please check callstack'



class _connection(pika.adapters.blocking_connection.BlockingConnection):
    def __init__(self, host, port=DEFAULT_PORT):
        # TODO when we upgrade to a version of Pika that supports heartbeats properly, use it.
        # Presently 0.9.5 does not.
        params = pika.ConnectionParameters(host=str(host), port=int(port))#, heartbeat=True)
        try:
            pika.adapters.blocking_connection.BlockingConnection.__init__(self, params)
        except socket.error:
            raise AMQPException('No AMQP server at ' + str(host) + ':' + str(port))
    def __enter__(self):
        return self
    def __exit__(self, *args, **kwargs):
        self.close()



# Imperative approach
# For sending any significant quantity of messages, these are terrible, as the
# overhead of settings up connections and channels for each message is
# devastating to efficiency.  They're pretty simple to use, though, and there
# are certain use cases.  Engineer beware, however.
def declare(exchange_name, exchange_type, host=DEFAULT_HOST, port=DEFAULT_PORT):
    with _connection(host, port) as connection:
        channel = connection.channel()
        channel.exchange_declare(
            exchange=exchange_name,
            type=exchange_type,
            durable=True,
            auto_delete=False
        )

def send(message, exchange_name, routing_key='#', host=DEFAULT_HOST, port=DEFAULT_PORT):
    with _connection(host, port) as connection:
        channel = connection.channel()
        channel.basic_publish(
            body=json.dumps(message),
            exchange=exchange_name,
            routing_key=routing_key
        )
    
def bind(exchange_name, queue_name, routing_key='#', host=DEFAULT_HOST, port=DEFAULT_PORT):
    with _connection(host, port) as connection:
        channel = connection.channel()
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
    
def receive(queue_name, host=DEFAULT_HOST, port=DEFAULT_PORT):
    with _connection(host, port) as connection:
        channel = connection.channel()
        while True:
            try:
                (method, header, body) = channel.basic_get(queue=queue_name)
                channel.basic_ack(method.delivery_tag)
                return json.loads(body)
            except (ValueError, AttributeError):
                pass

def listen(queue_name, callback, host=DEFAULT_HOST, port=DEFAULT_PORT):
    with _connection(host, port) as connection:
        channel = connection.channel()
        def _callback(channel, method, header, body):
            callback(json.loads(body))
            channel.basic_ack(method.delivery_tag)
        channel.basic_consume(_callback, queue=queue_name)
        channel.start_consuming()



# Class-based approach
class _persistently_connected(object):
    def __init__(self, **kwargs):
        hosts = kwargs.get('hosts', [kwargs.get('host', DEFAULT_HOST)])
        if isinstance(hosts, basestring):
            hosts = hosts.split(',')
        default_port = kwargs.get('port', DEFAULT_PORT)
        self._hosts = []
        for host in hosts:
            hostname = host.split(':')[0]
            try:
                port = int(host.split(':')[1])
            except IndexError:
                port = default_port
            self._hosts.append((hostname, str(port)))
        random.shuffle(self._hosts)
        # Subclasses should perform additional initialization steps while overriding this, presumably ending in a call to self._reconnect()
    def _reconnect(self, *args, **kwargs):
        self.__connection = None
        self.__channel = None
        # Subclasses should perform additional reconnection steps while overriding this.
    def _connection(self):
        if self.__connection is None:
            for (host, port) in self._hosts:
                try:
                    self.__connection = _connection(host, port)
                    self.__connection.add_on_close_callback(self._reconnect)
                    break
                except AMQPException:
                    pass
            else:
                raise AMQPException('No AMQP server at ' + ','.join([':'.join([hostname, port]) for (hostname, port) in self._hosts]))
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
        
        self._reconnect()
    def _reconnect(self, *args, **kwargs):
        _persistently_connected._reconnect(self)
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
            body=json.dumps(message, default=lambda x: "<non-serializable data>"),
            exchange=self._exchange_name,
            routing_key=routing_key
        )

class consumer(_persistently_connected):
    def __init__(self, **kwargs):
        _persistently_connected.__init__(self, **kwargs)
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
        # You must subscribe to a queue.  Almost everybody will want to
        # provide this parameter; however, if you truly don't care what
        # your queue is called (ie you don't care if you share it with
        # anyone else and you don't care if you can find it again), then
        # we're going to assume you want an auto-deleted exclusive queue.
        self._queue_name = kwargs.get('queue_name', None)
        self._temporary_and_exclusive = (self._queue_name is None)
        if self._temporary_and_exclusive:
            self._queue_name = '_'.join([os.uname()[1], 'pid', str(os.getpid())])
        self._routing_keys = kwargs.get('routing_keys', None)
        if 'routing_keys' not in kwargs and self._exchange_type == 'direct':
            self._routing_keys = ['#']
        
        self._reconnect()
    def _reconnect(self, *args, **kwargs):
        _persistently_connected._reconnect(self)
        # Always declare the queue.  If the queue already exists this is a
        # no-op.  If it does not, you'll be glad we declared it.
        self._channel().queue_declare(
            queue=self._queue_name,
            # In my experience these are the most useful sorts of queues.  If
            # you really want to configure another kind, complain loudly and
            # I'll make these configurable.
            durable=True,
            # If you are omitted a queue name, your queue is probably not that
            # important (we will let it be deleted when you stop consuming),
            # and probably you don't need to share it with anybody else (we'll
            # make sure it's exclusive)
            exclusive=self._temporary_and_exclusive,
            auto_delete=self._temporary_and_exclusive,
            # Notify when disconnected
            #consumer_cancel_notify=True,
            # Mirrored queue
            arguments={'x-ha-policy':'all'}
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
                    routing_key=str(routing_key)
                )
    # If you're creating an instance of this without subclassing, you can
    # call listen() and provide a callback method.
    def listen(self, callback):
        def _callback(channel, method, header, body):
            callback(json.loads(body))
            self._channel().basic_ack(method.delivery_tag)
        while True:
            try:
                self._channel().basic_consume(_callback, queue=self._queue_name)
                self._channel().start_consuming()
            except pika.exceptions.AMQPConnectionError:
                self._reconnect()
                continue
    # If you're subclassing this, you can implement the process method, and
    # call loop().
    def loop(self):
        self.listen(callback=self.process)
    def process(self, message):
        raise NotImplemented
    def purge(self):
        self._channel().queue_purge()