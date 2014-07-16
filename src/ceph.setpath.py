#!/usr/bin/python
# -*- mode:python -*-
# vim: ts=4 sw=4 smarttab expandtab
#

glop="""
sys.path.insert(0, "pypath") 
os.environ["PATH"] = "binpath:" + os.environ["PATH"]
"""
import sys
if sys.argv[1] not in sys.path:
  print glop.replace('pypath', sys.argv[1]).replace('binpath',sys.argv[2]),
