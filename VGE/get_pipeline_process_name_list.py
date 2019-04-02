#/*
# *    Virtual Grid Engine
# *
# *   (c) Copyright 2017-2019 Satoshi ITO, Masaaki Yadome, and Satoru MIYANO
# */

def get_pipeline_process_name_list(rank):
    #
    #
    pipeline_process_name_list=[]
    #
    # process name candidates which run on a master rank.
    #
    if "master" in rank: 
        pipeline_process_name_list.append("genomon_pipeline")
        pipeline_process_name_list.append("vge_connect")
        pipeline_process_name_list.append("vge")
    #
    # process name candidates which run on worker ranks.
    #
    elif "worker" in rank: 
        pipeline_process_name_list.append("split")
        pipeline_process_name_list.append("bamtofastq")
        pipeline_process_name_list.append("bwa")
        pipeline_process_name_list.append("bammarkduplicate")
        pipeline_process_name_list.append("blat")
        pipeline_process_name_list.append("samtools")
        pipeline_process_name_list.append("fisher")
        pipeline_process_name_list.append("mutfilter")
        pipeline_process_name_list.append("GenomonSV")
        pipeline_process_name_list.append("bamfilter")
        pipeline_process_name_list.append("EBFilter")
        pipeline_process_name_list.append("bamsort")
        pipeline_process_name_list.append("bammerge")
    #
    #
    return pipeline_process_name_list
    


