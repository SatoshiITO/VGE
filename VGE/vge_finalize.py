#/*
# *    Virtual Grid Engine
# *
# *   (c) Copyright 2017-2019 Satoshi ITO, Masaaki Yadome, and Satoru MIYANO
# */

from VGE.vge_task import vge_task

def vge_finalize():

   #
   # send goodbye message to VGE.
   #
   result = vge_task("byebye_from_pipeline",-1,"", "","multi")

   #
   #
   #
   return result
   #
   #
   #
  

