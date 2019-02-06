#!/bin/sh -x
#

qsub -N mysplit ./mysplit.sh

qsub -t 1-2:1 -hold_jid mysplit bwa_mapping.sh
