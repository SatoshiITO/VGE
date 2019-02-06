#$ -S /bin/bash
#$ -cwd
#$ -e log
#$ -o log
#$ -l s_vmem=8G,mem_req=8G
#$ -pe mpi 3-3

#
# set an environment
#
export PATH=~/.local/bin:$PATH
export PYTHONPATH=~/.local/lib/python2.7/site-packages/:${PYTHONPATH}

# 
# start VGE
#
mpiexec -n 3 vge &
vge_connect --start

#
# execute genomon_pipeline
#
time python ./samplecode.py

#
# stop VGE
#
vge_connect --stop
