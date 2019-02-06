#$ -S /bin/bash
#$ -cwd
#$ -e log
#$ -o log
#$ -l s_vmem=1G,mem_req=1G

LINES=500000

split -a 3 -d -l $LINES small1.fastq 1_
split -a 3 -d -l $LINES small2.fastq 2_
