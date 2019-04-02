#$ -S /bin/bash
#$ -cwd
#$ -e log
#$ -o log
#$ -masterl s_vmem=8G,mem_req=8G,os7
#$ -l s_vmem=2G,mem_req=2G,os7
#$ -pe mpi 3-3

#export VGE_CONF=$HOME

#
# clean
#
#mpiexec -n 3 cleanvge --verbose 0  2> clean.log 

#
# start VGE up
#
mpiexec -n 3 vge --nowrite_jobscript &

#
# wait for VGE to run
#
vge_connect --start 2> start.log

#
# run vge_sample
#
time ./vge_sample > stdout1.log 2> stderr1.log &

#
# wait for vge_sample(s) to finish
#
vge_connect --wait --monitor vge_sample --wait_maxtime 60 --sleep 1 >  wait_out.log 2> wait_err.log

#
# stop VGE
#
vge_connect --stop --force --target vge_test --stop_maxtime 1200 > stop_out.log 2> stop_err.log
