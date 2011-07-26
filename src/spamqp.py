#!/usr/bin/env python

import json
import os
import sys

import amqplib.client_0_8

class _channel(amqplib.client_0_8.channel.Channel):
    def __enter__(self):
        return self
    def __exit__(self, *args, **kwargs):
        self.close()

class _connection(amqplib.client_0_8.Connection):
    def __init__(self, host, port):
        amqplib.client_0_8.Connection.__init__(self, host=str(host) + ':' + str(port))
    def __enter__(self):
        return self
    def __exit__(self, *args, **kwargs):
        self.close()
    def channel(self):
        return _channel(self)

def declare(exchange_name, exchange_type, host='127.0.0.1', port=5672):
    with _connection(host, port) as connection:
        with connection.channel() as channel:
            channel.exchange_declare(exchange=exchange_name, type=exchange_type, durable=True, auto_delete=False)

def send(message, exchange_name, routing_key='#', host='127.0.0.1', port=5672):
    with _connection(host, port) as connection:
        with connection.channel() as channel:
            channel.basic_publish(json.dumps(message), exchange=exchange_name, routing_key=routing_key)
    
def bind(exchange_name, queue_name, routing_key='#', host='127.0.0.1', port=5672):
    with _connection(host, port) as connection:
        with connection.channel() as channel:
            channel.queue_declare(queue=queue_name, durable=True, exclusive=False, auto_delete=False)
            channel.queue_bind(queue=queue_name, exchange=exchange_name, routing_key=routing_key)
    
def receive(queue_name, host='127.0.0.1', port=5672):
    with _connection(host, port) as connection:
        with connection.channel() as channel:
            ret = None
            def _callback(message):
                ret = json.loads(message.body)
                channel.basic_ack(message.delivery_tag)
            consumer_tag = channel.basic_consume(queue=queue_name, callback=_callback)
            channel.wait()
            channel.basic_cancel(consumer_tag)
            return ret

def listen(queue_name, callback, host='127.0.0.1', port=5672):
    with _connection(host, port) as connection:
        with connection.channel() as channel:
            def _callback(message):
                callback(json.loads(message.body))
                channel.basic_ack(message.delivery_tag)
            channel.basic_consume(queue=queue_name, callback=_callback)
            while True:
                channel.wait()