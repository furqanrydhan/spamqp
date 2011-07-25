#!/usr/bin/env python
# -*- coding: utf-8 -*-

import setuptools
 
def setup():
    setuptools.setup(
        name='spamqp',
        version='0.1',
        description='StylePage tools: Python AMQP',
        author='mattbornski',
        url='http://github.com/stylepage/spamqp',
        package_dir={'': 'src'},
        py_modules=[
            'spamqp',
        ],
        install_requires=[
            # N.B. amqplib package version 0.6 implements client version 0.8
            'amqplib==0.6',
        ],
    )

if __name__ == '__main__':
    setup()