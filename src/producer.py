#!/usr/bin/env python

import amqplib.client_0_8

import _amqp

class producer(_amqp._amqp):
    def produce(self, message, routing_key='#'):
        if not self._initialized:
            self.initialize()
        def wrap(message):
            if not isinstance(message, amqplib.client_0_8.basic_message.Message):
                if isinstance(message, str) or isinstance(message, unicode):
                    return amqplib.client_0_8.Message(message)
                else:
                    raise TypeError('Message should be a string type (got ' + str(type(message)) + ')')
            else:
                return message
        with self.channel() as channel:
            channel.basic_publish(wrap(message), exchange='_'.join([self._exchange_name, self._exchange_type]), routing_key=routing_key)
            
if __name__ == '__main__':
    import sys
    p = producer(exchange_type='direct', exchange_name='test')
    while True:
        line = sys.stdin.readline()
        p.produce(line)