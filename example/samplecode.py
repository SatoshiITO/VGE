#!/usr/bin/env python

import os
import time
import subprocess, multiprocessing

from VGE.vge_conf import *
from VGE.vge_task import vge_task
from make_commands import *



#
# this is required to read vge conf
#
default_vge_conf = ""
default_vge_conf = os.getcwd() + "/vge.cfg"
if os.path.isfile(default_vge_conf):
  pass
elif "VGE_CONF" in os.environ:
  default_vge_conf_dir = os.environ["VGE_CONF"]
  default_vge_conf = default_vge_conf_dir + "/vge.cfg"
  if os.path.isfile(default_vge_conf):
    pass
vge_conf.read(default_vge_conf)
vge_conf_check()



#
# Split Fastq files
#
command=""
with open("mysplit.sh","r") as file:
  command=file.read()
max_task=2
basename_for_output="mysplit"
vge_task(command, max_task, basename_for_output, "")
print("--- mysplit has successflly accomplished.---")


#
# BWA mapping
#
command=""
with open("bwa_mapping.sh","r") as file:
  command=file.read()
max_task=2
basename_for_output="bwa_mapping"
vge_task(command, max_task, basename_for_output, "")
print("--- bwa_mapping has successflly accomplished.---")
