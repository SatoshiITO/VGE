# VGE
Virtual Grid Engine
Copyright (c) 2016 Satoshi ITO, Masaaki YADOME, and Satoru Miyano

1. General

Virtual Grid Engine (VGE) is a kind of middleware for running bioinformatics
software pipelines on large-scale supercomputers which do not support any
grid engine survices. VGE employs master-worker model. It first reserves
processors and/or cores by running the job which is parallelized by MPI, then
asign divided small tasks onto its worker processes. VGE is written in python.
