#!/usr/bin/env python

# Standard packages
import os
import sys
import argparse

# Third-party packages
from toil.job import Job

# Package methods
from ddb import configuration
from ddb_ngsflow import pipeline
from ddb_ngsflow.rna import hisat
from ddb_ngsflow.rna import stringtie


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
    transcripts_list = list()
    flags = ["keep_retained", "max_intron", "stranded"]

    # Per sample jobs
    for sample in samples:
        # Alignment and Refinement Stages
        align_job = Job.wrapJobFn(hisat.hisat_unpaired, config, sample, samples, flags,
                                  cores=int(config['hisat']['num_cores']),
                                  memory="{}G".format(config['hisat']['max_mem']))

        samples[sample]['bam'] = "{}.hisat.sorted.bam".format(sample)

        initial_st_job = Job.wrapJobFn(stringtie.stringtie_first, config, sample, samples, flags,
                                       cores=int(config['stringtie']['num_cores']),
                                       memory="{}G".format(config['stringtie']['max_mem']))

        transcripts_list.append("{}.stringtie_first.gtf".format(sample))

        # Create workflow from created jobs
        root_job.addChild(align_job)
        align_job.addChild(initial_st_job)

    transcripts_list_string = " ".join(transcripts_list)
    merge_job = Job.wrapJobFn(stringtie.stringtie_merge, config, samples, flags, transcripts_list_string,
                              cores=int(config['stringtie']['num_cores']),
                              memory="{}G".format(config['stringtie']['max_mem']))

    root_job.addFollowOn(merge_job)

    config['merged_transcript_reference'] = "{}.stringtie.merged.gtf".format(config['run_id'])

    for sample in samples:
        stringtie_job = Job.wrapJobFn(stringtie.stringtie, config, sample, samples, flags,
                                      cores=int(config['stringtie']['num_cores']),
                                      memory="{}G".format(config['stringtie']['max_mem']))
        merge_job.addChild(stringtie_job)

    # Start workflow execution
    Job.Runner.startToil(root_job, args)
