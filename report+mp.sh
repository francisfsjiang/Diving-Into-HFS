#!/bin/sh
mpost  -tex=tex report+mp0001
epstopdf --hires report+mp0001.mps
