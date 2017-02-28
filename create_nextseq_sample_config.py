#!/usr/bin/env python

# Standard packages
import sys
import glob
import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-o', '--output', help="Output samples configuration file name")
    args = parser.parse_args()
    args.logLevel = "INFO"

    files = glob.glob("*_R1_001.fastq.gz")

    with open(args.output, 'w') as outfile:
        for fastq_file in files:
            sections = fastq_file.split("_")
            library_name = "{}_{}_{}".format(sections[0], sections[1], sections[2])
            path = "/mnt/shared-data/dgaston_projects/L.Penney_Acadian_BRCA-13013"

            outfile.write("[{}]\n".format(library_name))
            outfile.write("fastq1: {}/{}_R1_001.fastq.gz\n".format(path, library_name))
            outfile.write("fastq2: {}/{}_R2_001.fastq.gz\n".format(path, library_name))
            outfile.write("library_name: {}\n".format(library_name))
            outfile.write("sample_name: {}\n".format(sections[0]))
            outfile.write("extraction: default\n")
            outfile.write("panel: exome\n")
            outfile.write("target_pool: default\n")
            outfile.write("sequencer: IWK_NextSeq\n")
            outfile.write("run_id: L.Penney_Acadian_BRCA-13013\n")
            outfile.write("num_libraries_in_run: 156\n\n")
