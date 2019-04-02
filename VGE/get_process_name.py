#/*
# *    Virtual Grid Engine
# *
# *   (c) Copyright 2017-2019 Satoshi ITO, Masaaki Yadome, and Satoru MIYANO
# */
import subprocess
def get_process_name(pid):
    #
    result=None
    #
    try:
        p = subprocess.Popen(["ps -o cmd= {}".format(pid)], stdout=subprocess.PIPE, shell=True)
        #
        result=str(p.communicate()[0])
        result=result.replace("\n",'')
        result=result.replace("\r",'')
        #
    except Exception,error:
        #
        result="Error"
    #
    #
    return result
    #
    #


