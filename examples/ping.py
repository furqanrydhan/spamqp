#!/usr/bin/env python

import spamqp

def ping():
    spamqp.declare(exchange_name='send_pings', exchange_type='direct')
    spamqp.bind(exchange_name='send_pings', queue_name='receive_pings', routing_key='#')
    
    spamqp.declare(exchange_name='send_pongs', exchange_type='direct')
    spamqp.bind(exchange_name='send_pongs', queue_name='receive_pongs', routing_key='#')
    
    message = 'ping'
    print message + '...'
    spamqp.send(message, exchange_name='send_pings', routing_key='#')
    print spamqp.receive('receive_pongs') + '!'

if __name__ == '__main__':
    ping()