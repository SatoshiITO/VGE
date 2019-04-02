#$ -S /bin/bash
#$ -cwd
#$ -e log
#$ -o log
#$ -l s_vmem=8G,mem_req=8G

FASTQ1=`printf 1_%03d ${NUM}`
FASTQ2=`printf 2_%03d ${NUM}`

~/bin/bwa mem GRCh37-lite.fa ${FASTQ1} ${FASTQ2} > ${NUM}.sam
