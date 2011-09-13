#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os.path
import setuptools
 
def setup():
    with open(os.path.join('src', 'spamqp.py'), 'r') as f:
        for line in f.readlines():
            if 'version' in line:
                try:
                    exec(line)
                    assert(isinstance(version, basestring))
                    break
                except (SyntaxError, AssertionError, NameError):
                    pass
    try:
        assert(isinstance(version, basestring))
    except (AssertionError, NameError):
        version = 'unknown'
    
    setuptools.setup(
        name='spamqp',
        version=version,
        description='StylePage tools: Python AMQP',
        author='mattbornski',
        url='http://github.com/stylepage/spamqp',
        package_dir={'': 'src'},
        py_modules=[
            'spamqp',
        ],
        install_requires=[
            'pika==0.9.5',
        ],
        dependency_links=[
            # The important things here:
            # 1. The URL should be accessible
            # 2. The URL should point to a page which _is_, or which clearly points _to_, the tarball/zipball/egg
            # 3. The URL should indicate which package and version it is
            'https://github.com/pika/pika/tarball/v0.9.5#egg=pika-0.9.5',
        ],
    )

if __name__ == '__main__':
    setup()