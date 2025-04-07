# CD33_Base_editing_Miseq
Scripts to analyze CD33 base edited Miseq sequencing data.

There are 3 python scripts to analyze the raw fastq data by merging the paired end reads and aligning it against the reference amplicon to get the frequencies of the edited reads and wild-type reads.

1. Run the genFilter.py script first.
   It takes a text file with names of the samples and a file with the primers, reference amplicon and the guide sequence as the input.
2. The genFilter.py script should create batch scripts for each sample to run the geneEditFilter.py and the geneEditOutput.py scripts.
