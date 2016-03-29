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
        # spawn_variant_job = Job.wrapJobFn(pipeline.spawn_variant_jobs)
        # normalization_job1 = Job.wrapJobFn(variation.vt_normalization, config, sample, "mutect",
        #                                    "{}.mutect.vcf".format(sample),
        #                                    cores=1,
        #                                    memory="{}G".format(config['gatk']['max_mem']))
        #
        # normalization_job2 = Job.wrapJobFn(variation.vt_normalization, config, sample, "scalpel",
        #                                    "{}.scalpel.vcf".format(sample),
        #                                    cores=1,
        #                                    memory="{}G".format(config['gatk']['max_mem']))
        #
        # normalization_job3 = Job.wrapJobFn(variation.vt_normalization, config, sample, "freebayes",
        #                                    "{}.freebayes.vcf".format(sample),
        #                                    cores=1,
        #                                    memory="{}G".format(config['gatk']['max_mem']))
        #
        # normalization_job4 = Job.wrapJobFn(variation.vt_normalization, config, sample, "vardict",
        #                                    "{}.vardict.vcf".format(sample),
        #                                    cores=1,
        #                                    memory="{}G".format(config['gatk']['max_mem']))
        #
        # normalization_job5 = Job.wrapJobFn(variation.vt_normalization, config, sample, "platypus",
        #                                    "{}.platypus.vcf".format(sample),
        #                                    cores=1,
        #                                    memory="{}G".format(config['gatk']['max_mem']))
        #
        # normalization_job6 = Job.wrapJobFn(variation.vt_normalization, config, sample, "pindel",
        #                                    "{}.pindel.vcf".format(sample),
        #                                    cores=1,
        #                                    memory="{}G".format(config['gatk']['max_mem']))
        #
        # callers = "mutect,scalpel,freebayes,vardict,platypus,pindel"
        #
        # merge_job = Job.wrapJobFn(variation.merge_variant_calls, config, sample, callers, (normalization_job1.rv(),
        #                                                                                    normalization_job2.rv(),
        #                                                                                    normalization_job3.rv(),
        #                                                                                    normalization_job4.rv(),
        #                                                                                    normalization_job5.rv(),
        #                                                                                    normalization_job6.rv()))

        gatk_annotate_job = Job.wrapJobFn(gatk.annotate_vcf, config, sample, "{}.merged.vcf".format(sample),
                                          samples[sample]['bam'],
                                          cores=int(config['gatk']['num_cores']),
                                          memory="{}G".format(config['gatk']['max_mem']))

        gatk_filter_job = Job.wrapJobFn(gatk.filter_variants, config, sample, gatk_annotate_job.rv(),
                                        cores=1,
                                        memory="{}G".format(config['gatk']['max_mem']))

        snpeff_job = Job.wrapJobFn(annotation.snpeff, config, sample, gatk_filter_job.rv(),
                                   cores=int(config['snpeff']['num_cores']),
                                   memory="{}G".format(config['snpeff']['max_mem']))

        gemini_job = Job.wrapJobFn(annotation.gemini, config, sample, snpeff_job.rv(),
                                   cores=int(config['gatk']['num_cores']),
                                   memory="{}G".format(config['gemini']['max_mem']))

        vcfanno_job = Job.wrapJobFn(annotation.vcfanno, config, sample,
                                    "{}.snpEff.{}.vcf".format(sample, config['snpeff']['reference']),
                                    cores=int(config['vcfanno']['num_cores']),
                                    memory="{}G".format(config['vcfanno']['max_mem']))

        # Create workflow from created jobs
        # root_job.addChild(spawn_variant_job)
        #
        # spawn_variant_job.addChild(normalization_job1)
        # spawn_variant_job.addChild(normalization_job2)
        # spawn_variant_job.addChild(normalization_job3)
        # spawn_variant_job.addChild(normalization_job4)
        # spawn_variant_job.addChild(normalization_job5)
        # spawn_variant_job.addChild(normalization_job6)
        # spawn_variant_job.addFollowOn(merge_job)

        # merge_job.addChild(gatk_annotate_job)
        root_job.addChild(gatk_annotate_job)
        gatk_annotate_job.addChild(gatk_filter_job)
        gatk_filter_job.addChild(snpeff_job)
        snpeff_job.addChild(vcfanno_job)

    # Start workflow execution
    Job.Runner.startToil(root_job, args)
