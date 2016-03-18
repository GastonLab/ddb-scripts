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
from ddb_ngsflow.align import bwa
from ddb_ngsflow import pipeline
from ddb_ngsflow.variation import variation

from ddb_ngsflow.variation import freebayes
from ddb_ngsflow.variation import mutect
from ddb_ngsflow.variation import platypus
from ddb_ngsflow.variation import vardict
from ddb_ngsflow.variation import scalpel
from ddb_ngsflow.variation.sv import pindel


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
    # root_job.addChildJobFn(utilities.run_fastqc, config, samples,
    #                        cores=1,
    #                        memory="{}G".format(config['fastqc']['max_mem']))

    # Per sample jobs
    for sample in samples:
        # Need to filter for on target only results somewhere as well
        spawn_normalization_job = Job.wrapJobFn(pipeline.spawn_variant_jobs)

        normalization_job1 = Job.wrapJobFn(variation.vt_normalization, config, sample, "freebayes",
                                           "{}.freebayes.vcf".format(sample),
                                           cores=1,
                                           memory="{}G".format(config['gatk']['max_mem']))

        normalization_job2 = Job.wrapJobFn(variation.vt_normalization, config, sample, "mutect",
                                           "{}.mutect.vcf".format(sample),
                                           cores=1,
                                           memory="{}G".format(config['gatk']['max_mem']))

        normalization_job3 = Job.wrapJobFn(variation.vt_normalization, config, sample, "vardict",
                                           "{}.vardict.vcf".format(sample),
                                           cores=1,
                                           memory="{}G".format(config['gatk']['max_mem']))

        normalization_job4 = Job.wrapJobFn(variation.vt_normalization, config, sample, "scalpel",
                                           "{}.scalpel.vcf".format(sample),
                                           cores=1,
                                           memory="{}G".format(config['gatk']['max_mem']))

        normalization_job5 = Job.wrapJobFn(variation.vt_normalization, config, sample, "platypus",
                                           "{}.platypus.vcf".format(sample),
                                           cores=1,
                                           memory="{}G".format(config['gatk']['max_mem']))

        normalization_job6 = Job.wrapJobFn(variation.vt_normalization, config, sample, "pindel",
                                           "{}.pindel.vcf".format(sample),
                                           cores=1,
                                           memory="{}G".format(config['gatk']['max_mem']))

        callers = "freebayes, mutect, vardict, scalpel, platypus, pindel"

        # merge_job = Job.wrapJobFn(variation.merge_variant_calls, config, sample, callers, (normalization_job1.rv(),
        #                                                                                    normalization_job2.rv(),
        #                                                                                    normalization_job3.rv(),
        #                                                                                    normalization_job4.rv(),
        #                                                                                    normalization_job5.rv(),
        #                                                                                    normalization_job6.rv()))

        # Removed temporarily until config generation script more easily adds in appropriate region files
        # on_target_job = Job.wrapJobFn(utilities.bcftools_filter_variants_regions, config, sample, samples,
        #                               merge_job.rv())

        # gatk_annotate_job = Job.wrapJobFn(gatk.annotate_vcf, config, sample, merge_job.rv(),
        #                                   "{}.recalibrated.sorted.bam".format(sample),
        #                                   cores=int(config['gatk']['num_cores']),
        #                                   memory="{}G".format(config['gatk']['max_mem']))
        #
        # gatk_filter_job = Job.wrapJobFn(gatk.filter_variants, config, sample, gatk_annotate_job.rv(),
        #                                 cores=1,
        #                                 memory="{}G".format(config['gatk']['max_mem']))
        #
        # snpeff_job = Job.wrapJobFn(annotation.snpeff, config, sample, gatk_filter_job.rv(),
        #                            cores=int(config['snpeff']['num_cores']),
        #                            memory="{}G".format(config['snpeff']['max_mem']))

        # Create workflow from created jobs
        root_job.addChild(spawn_normalization_job)

        spawn_normalization_job.addChild(normalization_job1)
        spawn_normalization_job.addChild(normalization_job2)
        spawn_normalization_job.addChild(normalization_job3)
        spawn_normalization_job.addChild(normalization_job4)
        spawn_normalization_job.addChild(normalization_job5)
        spawn_normalization_job.addChild(normalization_job6)

        # spawn_normalization_job.addFollowOn(merge_job)

        # merge_job.addChild(gatk_annotate_job)
        # on_target_job.addChild(gatk_annotate_job)
        # gatk_annotate_job.addChild(gatk_filter_job)
        # gatk_filter_job.addChild(snpeff_job)

    # Start workflow execution
    Job.Runner.startToil(root_job, args)
