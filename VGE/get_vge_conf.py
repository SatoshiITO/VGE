#/*
# *    Virtual Grid Engine
# *
# *   (c) Copyright 2017-2019 Satoshi ITO, Masaaki Yadome, and Satoru MIYANO
# */
from VGE.vge_conf import *

def get_vge_conf(section, option, default_value):
    #
    value = default_value
    #
    try:
       value = float(vge_conf.get(section,option))
       if value < 0.0:
           value = default_value
    except Exception,error:
       pass

    return value


