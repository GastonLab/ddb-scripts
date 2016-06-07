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
from ddb_ngsflow.rna import cufflinks


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
        # Merge alignments and run cufflinks
        merge_job = Job.wrapFn(gatk.merge_sam, config, sample, [samples[sample]['star'], samples[sample]['bowtie']],
                               cores=int(config['picard-merge']['num_cores']),
                               memory="{}G".format(config['picard-merge']['max_mem']))

        cufflinks_job = Job.wrapFn(cufflinks.cufflinks, config, sample, merge_job.rv(),
                                   cores=int(config['cufflinks']['num_cores']),
                                   memory="{}G".format(config['cufflinks']['max_mem']))

        # Create workflow from created jobs
        root_job.addChild(merge_job)
        merge_job.addChild(cufflinks_job)

    # Start workflow execution
    Job.Runner.startToil(root_job, args)
