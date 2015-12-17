#!/usr/bin/env bash

xterm -e python proposer.py &
xterm -e python proposer.py &
xterm -e python proposer.py &
xterm -e python proposer.py &

xterm -e python acceptor.py &
xterm -e python acceptor.py &
xterm -e python acceptor.py &

xterm -e python learner.py &
xterm -e python learner.py &
