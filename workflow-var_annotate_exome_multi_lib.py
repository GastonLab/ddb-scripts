#!/usr/bin/env python

# Standard packages
import sys
import argparse

# Third-party packages
from toil.job import Job

# Package methods
from ddb import configuration
from ddb_ngsflow import gatk
from ddb_ngsflow import annotation
from ddb_ngsflow import pipeline
from ddb_ngsflow.align import bwa
from ddb_ngsflow.variation import variation
from ddb_ngsflow.variation import haplotypecaller


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
    libraries = configuration.configure_samples(args.samples_file, config)
    samples = configuration.merge_library_configs_samples(libraries)

    # Workflow Graph definition. The following workflow definition should create a valid Directed Acyclic Graph (DAG)
    root_job = Job.wrapJobFn(pipeline.spawn_batch_jobs, cores=1)

    snpeff_job = Job.wrapJobFn(annotation.snpeff, config, config['project'],
                               "{}.filtered.vcf".format(config['project']),
                               cores=int(config['snpeff']['num_cores']),
                               memory="{}G".format(config['snpeff']['max_mem']))

    # root_job.addFollowOn(joint_call_job)
    root_job.addFollowOn(snpeff_job)

    # Start workflow execution
    Job.Runner.startToil(root_job, args)
