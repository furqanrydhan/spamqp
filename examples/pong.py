#!/usr/bin/env python

import spamqp

def pong():
    spamqp.declare(exchange_name='send_pings', exchange_type='direct')
    spamqp.bind(exchange_name='send_pings', queue_name='receive_pings', routing_key='#')
    
    spamqp.declare(exchange_name='send_pongs', exchange_type='direct')
    spamqp.bind(exchange_name='send_pongs', queue_name='receive_pongs', routing_key='#')
    
    print spamqp.receive('receive_pings') + '!'
    message = 'pong'
    print message + '...'
    spamqp.send(message, exchange_name='send_pongs', routing_key='#')
    

if __name__ == '__main__':
    pong()