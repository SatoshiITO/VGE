#/*
# *    Virtual Grid Engine
# *
# *   (c) Copyright 2017-2019 Satoshi ITO, Masaaki Yadome, and Satoru MIYANO
# */
import subprocess
def get_process_list(machine_type):
    #
    # machine_type :
    #   kei       ...  K computer (a supercomputer in RIKEN)
    #   linux      ... generic linux machine

    import os,pwd

    #
    # get my username
    #
    username=""
    username=pwd.getpwuid( os.getuid() ).pw_name

    #
    # get a process list on a current node
    #
    process_list=None
    #
    command=[]
    if machine_type is "kei":
       #
       command=["ps","uwwx"]

    elif machine_type is "linux":
       command=["ps","uwwx"]
       #
    else:     
       #
       command=["ps","uwwx"]

    process_list = subprocess.Popen(command, stdout=subprocess.PIPE).communicate()[0]


    #
    #
    return process_list
    #
    #


