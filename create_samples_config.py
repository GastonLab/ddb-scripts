#!/usr/bin/env python

# Standard packages
import sys
import argparse
import ConfigParser

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input', help="Input SamplesSheet")
    parser.add_argument('-o', '--output', help="Output samples configuration file name")
    parser.add_argument('-p', '--panel', help="Panel name")
    parser.add_argument('-s', '--sequencer', help="Sequencer name")
    parser.add_argument('-r', '--run_id', help="Run ID")
    args = parser.parse_args()
    args.logLevel = "INFO"

    config = ConfigParser.SafeConfigParser()
    config.read(args.input)

    num_samples = 0
    for sample_line in config.options('Data'):
        if not sample_line.startswith("Sample_ID,"):
            num_samples += 1

    with open(args.output, 'w') as output:
        lib_num = 1
        for sample_line in config.options('Data'):
            if not sample_line.startswith("Sample_ID,"):
                info = sample_line.split(',')
                sample_name = info[1]
                library_name = "{}_S{}".format(sample_name, lib_num)

                sys.stdout.write("Processing library {}\n".format(library_name))
                output.write("[{lib}]\nfastq1: {lib}_L001_R1_001.fastq.gz\n"
                             "fastq2: {lib}_L001_R1_001.fastq.gz\nlibrary_name: {lib}\n"
                             "sample_name: {sample}\nextraction: default\npanel: {panel}\n"
                             "target_pool: {pool}\nsequencer: {seq}\nrun_id: {run}\nnum_libraries_in_run: {num}\n"
                             "".format(lib=library_name, sample=sample_name, panel=args.panel, pool=info[-2],
                                       seq=args.sequencer, run=args.run_id, num=num_samples))

                if info[-2] == 'A':
                    output.write("regions: /mnt/shared-data/Resources/MiSeqPanels/tst15-regionsA.bed\n")
                    output.write("vcfanno_config: /mnt/shared-data/ddb-configs/annotation/vcfanno_ts15A.conf\n")
                elif info[-2] == 'B':
                    output.write("regions: /mnt/shared-data/Resources/MiSeqPanels/tst15-regionsB.bed\n")
                    output.write("vcfanno_config: /mnt/shared-data/ddb-configs/annotation/vcfanno_ts15B.conf\n")
                else:
                    sys.stderr.write("Sample line {} has unknown pool {}\n".format(sample_line, info[-2]))

                lib_num += 1
