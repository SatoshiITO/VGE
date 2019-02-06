#
# local env
#
def local_env():
  return """
#!/bin/bash

export HOME=`pwd`
source /work/system/Env_base_1.2.0-20-1
export FLIB_CPUBIND=chip_pack
export FLIB_CNTL_BARRIER_ERR=FALSE
export FLIB_FASTOMP=FALSE
export PARALLEL=8
export OMP_NUM_THREADS=8
export LANG=C
export LC_ALL=C
export PATH=/opt/klocal/gcc/bin:$PATH
export LD_LIBRARY_PATH=/opt/klocal/lib:/opt/klocal/gcc/lib64:$LD_LIBRARY_PATH
export LD_LIBRARY_PATH=$HOME/public/lib:$HOME/public/python_2.7.11/lib:$LD_LIBRARY_PATH
export PATH=$HOME/public/bin:$HOME/public/python_2.7.11/bin:$PATH
export PATH=.:$PATH
export PYTHONHOME=$HOME/public/python_2.7.11

SGE_TASK_ID=`expr VGE_BULKJOB_ID + 1`

"""



#
# shell script for fastq splitter
#
def fastq_splitter():
  command=local_env()
  command+="""
if [ -f ./testdata/1_${SGE_TASK_ID}.gz ]; then
    zcat ./testdata/1_${SGE_TASK_ID}.gz | split -a 4 -d -l 1000000 - ./testdata/${SGE_TASK_ID}_
    status=("${PIPESTATUS[@]}")
    [ ${PIPESTATUS[0]} -ne 0 ] || echo ${PIPESTATUS[0]}
else
    split -a 4 -d -l 160000 ./testdata/1_${SGE_TASK_ID}.fastq ./testdata/${SGE_TASK_ID}_ || exit $?
fi

ls -1 ./testdata/${SGE_TASK_ID}_[0-9][0-9][0-9][0-9] | while read filename; do
    mv $filename $filename.fastq_split || exit $?
done

sleep 1
"""
  return command



#
# shell script for BWA
#
def bwa_align():
  command=local_env()
  command+="""
tmp_num=`expr ${SGE_TASK_ID} - 1`
num=`printf "%04d" ${tmp_num}`

./public/bin/bwa mem -T 0 -t 8 ./database/GRCh37/GRCh37.fa ./testdata/1_${num}.fastq_split ./testdata/2_${num}.fastq_split > ./testdata/sample_tumor_${num}.bwa.sam || exit $?

./public/bin/bamsort index=1 level=1 inputthreads=2 outputthreads=2 calmdnm=1 calmdnmrecompindentonly=1 calmdnmreference=./database/GRCh37/GRCh37.fa tmpfile=./testdata/sample_tumor_${num}.sorted.bam.tmp inputformat=sam indexfilename=./testdata/sample_tumor_${num}.sorted.bam.bai I=./testdata/sample_tumor_${num}.bwa.sam O=./testdata/sample_tumor_${num}.sorted.bam

sleep 1
"""
  return command



#
# shell script for markduplicates
#
def markduplicates():
  command=local_env()
  command+="""
ulimit -a
ulimit -n 1024
ulimit -a

#./public/bin/bammarkduplicates M=./testdata/sample_tumor.markdup.metrics tmpfile=./testdata/sample_tumor.markdup.tmp markthreads=8 rewritebam=1 rewritebamlevel=1 index=1 md5=1  I=./testdata/sample_tumor_0000.sorted.bam O=./testdata/sample_tumor.markdup.bam

./public/bin/bammarkduplicates M=./testdata/sample_tumor.markdup.metrics tmpfile=./testdata/sample_tumor.markdup.tmp markthreads=8 rewritebam=1 rewritebamlevel=1 index=1 md5=1 I=./testdata/sample_tumor_0000.sorted.bam I=./testdata/sample_tumor_0001.sorted.bam O=./testdata/sample_tumor.markdup.bam

sleep 1
"""
  return command
