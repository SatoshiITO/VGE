#!/bin/sh -x
#

echo "Download GRCh37-lite.fa from NIH ftp server."
#
### Prepare the reference fasta
#
if [ ! -f GRCh37-lite.fa ]; then

    if [ -f GRCh37-lite.fa.gz ]; then
	echo "eed6d4df26908457ae74fac4976f2699  GRCh37-lite.fa.gz" > md5.txt

	if [ `md5sum --status -c md5.txt; echo $?` ]; then
	    echo "Download succeeded."
	else
	    wget ftp://ftp.ncbi.nih.gov/genomes/archive/old_genbank/Eukaryotes/vertebrates_mammals/Homo_sapiens/GRCh37/special_requests/GRCh37-lite.fa.gz    
	fi

	rm -f md5.txt
	gunzip GRCh37-lite.fa.gz
    fi

else

   time  ~/bin/bwa index GRCh37-lite.fa

fi
