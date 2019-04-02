#!/bin/env python

from distutils.core import setup

setup(name='VGE',
      version='2.0.0',
      description='Virtual Grid Engine for running bioinformatics pipelines on MPI-base supercomputers',
      author='Satoshi ITO, Masaaki YADOME, and Satoru MIYANO',
      author_email='sito.public@gmail.com',
      url='https://github.com/SatoshiITO/VGE',
      packages=['VGE'],
      data_files=[('bin',['vge.cfg'])],
      scripts=['vge','vge_connect','cleanvge'],
     )
