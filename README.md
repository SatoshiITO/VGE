# VGE: Virtual Grid Engine
Copyright (c) 2017 Satoshi ITO, Masaaki YADOME, and Satoru MIYANO

## 1. General

Virtual Grid Engine (VGE) is a kind of middleware for running bioinformatics
software pipelines on large-scale supercomputers which do not support any
grid engine survices. VGE employs master-worker model. It first reserves
processors and/or cores by running the job which is parallelized by MPI, then
asign divided small tasks onto its worker processes. VGE is written in python.

## 2. Prerequisite

VGE uses Message Passing Library through MPI4PY module. It also uses socket
communication between VGE master process and VGE jobcontroler process.
Dependent software and their versions are below:

 - Python (2.7>=)
 - MPI4PY (2.0.0>=)
 - MPICH or OpenMPI (2.0>=)

## 3. Install

Download source archive and extract it. Then type following commands:
```
 $ cd VGE  
 $ python setup.py install --user
```
## 4. Reference
S. Ito et al., "Virtual Grid Engine: Accelerating thousands of omics sample analyses using large-scale supercomputers," 2018 IEEE International Conference on Bioinformatics and Biomedicine (BIBM), Madrid, Spain, 2018, pp. 387-392. doi: 10.1109/BIBM.2018.8621285
