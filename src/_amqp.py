#!/usr/bin/env python

import amqplib.client_0_8

# A primer on AMQP routing
# Producers send messages to exchanges.  Each message has a routing key attached.
# Exchanges deliver messages to queues.  Many queues may receive the same message based on the routing key.
# Queues deliver messages to consumers.  Each message in a queue will be delivered exactly once.
# Thus, each discrete task should define a queue, and cause that queue to subscribe to the relevant messages.
# The queue will divide the work among the consumers without duplication.
# Only consumers need a queue name.  The consumers queue name should be the same across all consumers for
# the task.
# Both producers and consumers need an exchange name.  Producers need to determine the explicit routing key
# for each message.  Consumers need to determine which pattern routing keys they need to bind their queue to.
class _amqp(object):
    _settings = None
    _amqp_connections = None
    _initialized = False
    _exchange_name = None
    _exchange_type = None
    def __init__(self, *args, **kwargs):
        self._settings = kwargs.get('settings', {})
        self._amqp_connections = {}
        if self._exchange_type is None:
            if 'exchange_type' in kwargs:
                self._exchange_type = kwargs['exchange_type']
                del kwargs['exchange_type']
        if self._exchange_name is None:
            if 'exchange_name' in kwargs:
                self._exchange_name = kwargs['exchange_name']
                del kwargs['exchange_name']
    def initialize(self):
        if self._exchange_type is None:
            self._exchange_type = 'direct'
        with self.channel() as channel:
            # The exchange will receive our messages.
            # In order to head off duplicate conflicting exchange definitions, this framework will mandate that the exchange type
            # be included in the exchange name.
            channel.exchange_declare(exchange='_'.join([self._exchange_name, self._exchange_type]), type=self._exchange_type, durable=True, auto_delete=False)
        self._initialized = True
    # Connection pooling:
    def connect(self, host=None, port=None):
        # Configurable connection
        # A host:port combo, if specified, will be used.
        # Otherwise, we will check and see if a default was configured in one of the config files or arguments.
        # Otherwise, we'll use a sane default.
        _host = host or self._settings.get('amqp', {}).get('host', None) or '127.0.0.1'
        _port = port or self._settings.get('amqp', {}).get('port', None) or '5672'
        _host = str(_host)
        _port = str(_port)
        if self._amqp_connections.get((_host, _port), None) is None or self._amqp_connections[(_host, _port)].channels is None:
            self._amqp_connections[(_host, _port)] = amqplib.client_0_8.Connection(host=(_host + ':' + _port))
        return self._amqp_connections[(_host, _port)]
    def channel(self):
        class amqp_channel(object):
            def __init__(self, connection):
                self._connection = connection
                self._channel = None
            def __enter__(self):
                if self._channel is None:
                    self._channel = self._connection.channel()
                return self._channel
            def __exit__(self, *args, **kwargs):
                if self._channel is not None:
                    self._channel.close()
        return amqp_channel(self.connect())