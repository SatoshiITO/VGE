#/*
# *    Virtual Grid Engine
# *
# *   (c) Copyright 2017-2019 Satoshi ITO, Masaaki Yadome, and Satoru MIYANO
# */
import os

def check_process_status(process_id):
    #
    # if a target process is alive then return True
    #
    try:
        #
        os.kill(process_id, 0) # 
        #
    except OSError:
        #
        return False # no such process!
        #
    else:
        #
        return True # alive
        #


