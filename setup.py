#!/bin/env python

from distutils.core import setup

setup(name='VGE',
      version='1.0',
      description='Virtual Grid Engine for running bioinformatics pipelines on MPI-base supercomputers',
      author='Koji HASEGAWA, Hikaru INOUE, Masaaki YADOME, and Satoshi ITO',
      author_email='sito.public@gmail.com',
      url='https://github.com/SatoshiITO/VGE',
      packages=['VGE'],
      data_files=[('bin',['vge.cfg'])],
      scripts=['vge','vge_connect'],
     )
