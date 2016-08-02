#!/usr/bin/env python

# Standard packages
import os
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
        # Alignment and Refinement Stages
        flags = ("keep_retained", "limit_bam_sort_ram", "cufflinks")

        align_job = Job.wrapJobFn(star.star_unpaired, config, sample, samples, flags,
                                  cores=int(config['star']['num_cores']),
                                  memory="{}G".format(config['star']['max_mem']))

        samples[sample]['star'] = "{}.star.Aligned.sortedByCoord.out.bam".format(sample)
        samples[sample]['unmapped_fastq'] = "{}.star.Unmapped.out.mate1".format(sample)

        bowtie_job = Job.wrapJobFn(bowtie.bowtie_unpaired, config, sample, samples, flags,
                                   cores=int(config['bowtie']['num_cores']),
                                   memory="{}G".format(config['bowtie']['max_mem']))

        samples[sample]['bowtie'] = "{}.bowtie.sam".format(sample)

        input_bams = list()
        input_bams.append(samples[sample]['star'])
        input_bams.append(samples[sample]['bowtie'])

        merge_job = Job.wrapJobFn(gatk.merge_sam, config, sample, input_bams,
                               cores=int(config['picard-merge']['num_cores']),
                               memory="{}G".format(config['picard-merge']['max_mem']))

        working_dir = os.getcwd()
        samples[sample]['bam'] = os.path.join(working_dir, "{}.merged.sorted.bam".format(sample))

        cufflinks_job = Job.wrapJobFn(cufflinks.cufflinks, config, sample, samples,
                                      cores=int(config['cufflinks']['num_cores']),
                                      memory="{}G".format(config['cufflinks']['max_mem']))

        # Create workflow from created jobs
        # root_job.addChild(align_job)
        # align_job.addChild(bowtie_job)
        # bowtie_job.addChild(merge_job)
        # align_job.addChild(merge_job)
        # merge_job.addChild(cufflinks_job)
        # root_job.addChild(cufflinks_job)

    # cuffmerge_job = Job.wrapJobFn(cufflinks.cuffmerge, config, "blah", samples, "manifest.txt",
    #                               cores=int(config['cuffmerge']['num_cores']),
    #                               memory="{}G".format(config['cuffmerge']['max_mem']))
    #
    # root_job.addFollowOn(cuffmerge_job)

    config['merged_transcript_reference'] = "./merged_asm/merged.gtf"

    for sample in samples:
        cuffquant_job = Job.wrapJobFn(cufflinks.cuffquant, config, sample, samples,
                                      cores=int(config['cuffquant']['num_cores']),
                                      memory="{}G".format(config['cuffquant']['max_mem']))
        root_job.addChild(cuffquant_job)

    # Start workflow execution
    Job.Runner.startToil(root_job, args)
