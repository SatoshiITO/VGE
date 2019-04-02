#/*
# *    Virtual Grid Engine
# *
# *   (c) Copyright 2017-2019 Satoshi ITO, Masaaki Yadome, and Satoru MIYANO
# */

import os

from VGE.pipeline_parent_pid import *
from VGE.vge_task import vge_task
from VGE.vge_conf import *

def vge_init():

   #
   # get a parent pid and deposit it
   #
   parent_pid = os.getpid()
   pipeline_parent_pid_deposit(parent_pid)

   #
   # read a VGE config file 
   #
   default_vge_conf = ""
   default_vge_conf = os.getcwd() + "/vge.cfg"
   if os.path.isfile(default_vge_conf):
       #
       # found!  
       #
       pass
       #
       #
   elif "VGE_CONF" in os.environ:
       #
       # search it at a second location
       #
       default_vge_conf_dir = os.environ["VGE_CONF"]
       default_vge_conf = default_vge_conf_dir + "/vge.cfg"
       if os.path.isfile(default_vge_conf):
          pass
   #
   #
   vge_conf.read(default_vge_conf)
   vge_conf_check()

   #
   # send a hello message to VGE.
   #
   result = vge_task("hello_from_pipeline",-1,"", "","multi")

   #
   #
   #
   return result
   #
   #
   #
  

