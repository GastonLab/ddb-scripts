#!/usr/bin/env python

# Standard packages
import sys
import argparse

# Third-party packages
from toil.job import Job

# Package methods
from ddb import configuration
from ddb_ngsflow import pipeline
from ddb_ngsflow.variation import variation


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
        # Need to filter for on target only results somewhere as well
        callers = "freebayes,mutect,vardict,scalpel,platypus,pindel"
        # callers = "freebayes,mutect,vardict,scalpel"

        merge_job = Job.wrapJobFn(variation.merge_variant_calls, config, sample, callers, ("{}.freebayes.normalized.vcf".format(sample),
                                                                                           "{}.mutect.normalized.vcf".format(sample),
                                                                                           "{}.vardict.normalized.vcf".format(sample),
                                                                                           "{}.scalpel.normalized.vcf".format(sample),
                                                                                           "{}.platypus.normalized.vcf".format(sample),
                                                                                           "{}.pindel.normalized.vcf".format(sample)
                                                                                           ),
                                  cores=int(config['ensemble']['num_cores']),
                                  memory="{}G".format(config['ensemble']['max_mem']))

        # Create workflow from created jobs
        root_job.addChild(merge_job)

    # Start workflow execution
    Job.Runner.startToil(root_job, args)
