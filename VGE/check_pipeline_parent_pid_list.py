#/*
# *    Virtual Grid Engine
# *
# *   (c) Copyright 2017-2019 Satoshi ITO, Masaaki Yadome, and Satoru MIYANO
# */

def  check_pipeline_parent_pid_list(process_id,pipeline_parent_pid_list,icount):
   #
   #
   check_single_mode=False
   flag_goahead=True
   #
   #
   if isinstance(pipeline_parent_pid_list,list) or isinstance(pipeline_parent_pid_list,dict):
     if len(pipeline_parent_pid_list) > 0:
        for pipeline_pid in pipeline_parent_pid_list.keys():
           if int(pipeline_pid) > 0:
              if not pipeline_parent_pid_list[pipeline_pid]:
                 flag_goahead=False
                 break
           else:
              # this Pipeline is not identified. (-1)
              check_single_mode=True
              if not pipeline_parent_pid_list[pipeline_pid]:
                 flag_goahead=False
     else:
        # pipeline_parent_pid_list is empty...  VGE doesn't recieve any job yet from Pipeline, or Pipeline was not running?
        flag_goahead=False

   else:
      # vge_task did not return a right answer?
      flag_goahead=False
      pass

   #
   return flag_goahead,check_single_mode
   #



