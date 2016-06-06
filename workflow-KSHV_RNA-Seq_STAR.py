#!/usr/bin/env python

# Standard packages
import sys
import argparse

# Third-party packages
from toil.job import Job

# Package methods
from ddb import configuration
from ddb_ngsflow import gatk
from ddb_ngsflow import pipeline
from ddb_ngsflow.rna import star
from ddb_ngsflow.rna import bowtie


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--samples_file', help="Input configuration file for samples")
    parser.add_argument('-c', '--configuration', help="Configuration file for various settings")
    Job.Runner.addToilOptions(parser)
    args = parser.parse_args()
    args.logLevel = "INFO"

    sys.stdout.write("Parsing configuration data\n")
    config = configuration.configure_runtime(args.configuration)

    sys.stdout.write("Parsing sample data\n")
    samples = configuration.configure_samples(args.samples_file, config)

    # Workflow Graph definition. The following workflow definition should create a valid Directed Acyclic Graph (DAG)
    root_job = Job.wrapJobFn(pipeline.spawn_batch_jobs, cores=1)

    # Per sample jobs
    for sample in samples:
        # Alignment and Refinement Stages
        flags = ("local", "2-pass")

        align_job = Job.wrapJobFn(star.star_unpaired, config, sample, samples, flags,
                                  cores=int(config['star']['num_cores']),
                                  memory="{}G".format(config['star']['max_mem']))

        outroot = align_job.rv()
        samples[sample]['unmapped_fastq'] = "{}Unmapped.out.mate1".format(outroot)

        bowtie2_job = Job.wrapJobFn(bowtie.bowtie_unpaired, config, sample, samples, flags,
                                    cores=int(config['bowtie']['num_cores']),
                                    memory="{}G".format(config['bowtie']['max_mem']))

        mapped_sams = [align_job.rv(), bowtie2_job.rv()]

        merge_job = Job.wrapFn(gatk.merge_sam, config, sample, mapped_sams,
                               cores=int(config['picard-merge']['num_cores']),
                               memory="{}G".format(config['picard-merge']['max_mem']))

        # cufflinks_job = Job.wrapFn()
        #
        # cuffmerge_job = Job.wrapFn()
        #
        # cuffquant_job = Job.wrapFn()
        #
        # cuffnorm_job = Job.wrapFn()

        # Create workflow from created jobs
        root_job.addChild(align_job)
        align_job.addChild(bowtie2_job)
        bowtie2_job.addChild(merge_job)
        # merge_job.addChild(cufflinks_job)
        # cufflinks_job.addChild(cuffmerge_job)
        # cuffmerge_job.addChild(cuffquant_job)
        # cuffquant_job.addChild(cuffnorm_job)

    # Start workflow execution
    Job.Runner.startToil(root_job, args)
