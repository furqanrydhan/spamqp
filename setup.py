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
            'pika',
        ],
        dependency_links=[
            'http://github.com/pika/pika/tarball/master#egg=pika',
        ],
    )

if __name__ == '__main__':
    setup()