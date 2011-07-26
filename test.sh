#!/bin/bash

ENV=/tmp/testenv_$RANDOM

virtualenv --no-site-packages $ENV
source $ENV/bin/activate
pip install -e .
python examples/ping.py &
python examples/pong.py
deactivate
rm -rf $ENV