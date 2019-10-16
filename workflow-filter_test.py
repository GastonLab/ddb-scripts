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
from ddb_ngsflow import annotation
from ddb_ngsflow import pipeline
from ddb_ngsflow.align import bwa
from ddb_ngsflow.qc import qc
from ddb_ngsflow.coverage import sambamba
from ddb_ngsflow.variation import variation
from ddb_ngsflow.variation import freebayes
from ddb_ngsflow.variation import mutect
from ddb_ngsflow.variation import platypus
from ddb_ngsflow.variation import vardict
from ddb_ngsflow.variation import scalpel
from ddb_ngsflow.variation.sv import pindel


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--samples_file',
                        help="Input configuration file for samples")
    parser.add_argument('-c', '--configuration',
                        help="Configuration file for various settings")
    Job.Runner.addToilOptions(parser)
    args = parser.parse_args()
    args.logLevel = "INFO"

    sys.stdout.write("Parsing configuration data\n")
    config = configuration.configure_runtime(args.configuration)

    sys.stdout.write("Parsing sample data\n")
    samples = configuration.configure_samples(args.samples_file, config)

    # Workflow Graph definition. The following workflow definition should
    # create a valid Directed Acyclic Graph (DAG)
    root_job = Job.wrapJobFn(pipeline.spawn_batch_jobs, cores=1)

    fastqc_job = Job.wrapJobFn(qc.run_fastqc, config, samples)

    # Per sample jobs
    for sample in samples:
        spawn_filter_job = Job.wrapJobFn(pipeline.spawn_variant_jobs)

        bgzip_tabix_job1 = Job.wrapJobFn(variation.bgzip_tabix_vcf, config,
                                         sample, "freebayes",
                                         "{}.freebayes.normalized.vcf".format(sample),
                                         cores=1,
                                         memory="{}G".format(config['gatk']['max_mem']))

        add_contig_job1 = Job.wrapJobFn(variation.add_refcontig_info_header,
                                        config, sample, "freebayes",
                                        "{}.freebayes.normalized.vcf.gz".format(sample),
                                        cores=1,
                                        memory="{}G".format(config['gatk']['max_mem']))

        qual_filter_job1 = Job.wrapJobFn(variation.filter_low_support_variants,
                                         config, sample, "freebayes",
                                         "{}.freebayes.rehead.vcf.gz".format(sample),
                                         cores=1,
                                         memory="{}G".format(config['gatk']['max_mem']))

        bgzip_tabix_job2 = Job.wrapJobFn(variation.bgzip_tabix_vcf, config,
                                         sample, "mutect",
                                         "{}.mutect.normalized.vcf".format(sample),
                                         cores=1,
                                         memory="{}G".format(config['gatk']['max_mem']))

        add_contig_job2 = Job.wrapJobFn(variation.add_refcontig_info_header,
                                        config, sample, "mutect",
                                        "{}.mutect.normalized.vcf.gz".format(sample),
                                        cores=1,
                                        memory="{}G".format(config['gatk']['max_mem']))

        qual_filter_job2 = Job.wrapJobFn(variation.filter_low_support_variants,
                                         config, sample, "mutect",
                                         "{}.mutect.rehead.vcf.gz".format(sample),
                                         cores=1,
                                         memory="{}G".format(config['gatk']['max_mem']))

        bgzip_tabix_job3 = Job.wrapJobFn(variation.bgzip_tabix_vcf, config,
                                         sample, "vardict",
                                         "{}.vardict.normalized.vcf".format(sample),
                                         cores=1,
                                         memory="{}G".format(config['gatk']['max_mem']))

        add_contig_job3 = Job.wrapJobFn(variation.add_refcontig_info_header,
                                        config, sample, "vardict",
                                        "{}.vardict.normalized.vcf.gz".format(sample),
                                        cores=1,
                                        memory="{}G".format(config['gatk']['max_mem']))

        qual_filter_job3 = Job.wrapJobFn(variation.filter_low_support_variants,
                                         config, sample, "vardict",
                                         "{}.vardict.rehead.vcf.gz".format(sample),
                                         cores=1,
                                         memory="{}G".format(config['gatk']['max_mem']))

        bgzip_tabix_job4 = Job.wrapJobFn(variation.bgzip_tabix_vcf, config,
                                         sample, "scalpel",
                                         "{}.scalpel.normalized.vcf".format(sample),
                                         cores=1,
                                         memory="{}G".format(config['gatk']['max_mem']))

        add_contig_job4 = Job.wrapJobFn(variation.add_refcontig_info_header,
                                        config, sample, "scalpel",
                                        "{}.scalpel.normalized.vcf.gz".format(sample),
                                        cores=1,
                                        memory="{}G".format(config['gatk']['max_mem']))

        qual_filter_job4 = Job.wrapJobFn(variation.filter_low_support_variants,
                                         config, sample, "scalpel",
                                         "{}.scalpel.rehead.vcf.gz".format(sample),
                                         cores=1,
                                         memory="{}G".format(config['gatk']['max_mem']))

        bgzip_tabix_job5 = Job.wrapJobFn(variation.bgzip_tabix_vcf, config,
                                         sample, "platypus",
                                         "{}.platypus.normalized.vcf".format(sample),
                                         cores=1,
                                         memory="{}G".format(config['gatk']['max_mem']))

        add_contig_job5 = Job.wrapJobFn(variation.add_refcontig_info_header,
                                        config, sample, "platypus",
                                        "{}.platypus.normalized.vcf.gz".format(sample),
                                        cores=1,
                                        memory="{}G".format(config['gatk']['max_mem']))

        qual_filter_job5 = Job.wrapJobFn(variation.filter_low_support_variants,
                                         config, sample, "platypus",
                                         "{}.platypus.rehead.vcf.gz".format(sample),
                                         cores=1,
                                         memory="{}G".format(config['gatk']['max_mem']))

        bgzip_tabix_job6 = Job.wrapJobFn(variation.bgzip_tabix_vcf, config,
                                         sample, "pindel",
                                         "{}.pindel.normalized.vcf".format(sample),
                                         cores=1,
                                         memory="{}G".format(config['gatk']['max_mem']))

        add_contig_job6 = Job.wrapJobFn(variation.add_refcontig_info_header,
                                        config, sample, "pindel",
                                        "{}.pindel.normalized.vcf.gz".format(sample),
                                        cores=1,
                                        memory="{}G".format(config['gatk']['max_mem']))

        qual_filter_job6 = Job.wrapJobFn(variation.filter_low_support_variants,
                                         config, sample, "pindel",
                                         "{}.pindel.rehead.vcf.gz".format(sample),
                                         cores=1,
                                         memory="{}G".format(config['gatk']['max_mem']))

        callers = "freebayes,mutect,vardict,scalpel,platypus,pindel"

        merge_job = Job.wrapJobFn(variation.merge_variant_calls, config, sample, callers, (qual_filter_job1.rv(),
                                                                                           qual_filter_job2.rv(),
                                                                                           qual_filter_job3.rv(),
                                                                                           qual_filter_job4.rv(),
                                                                                           qual_filter_job5.rv(),
                                                                                           qual_filter_job6.rv()))

        gatk_annotate_job = Job.wrapJobFn(gatk.annotate_vcf, config, sample, merge_job.rv(),
                                          "{}.recalibrated.sorted.bam".format(sample),
                                          cores=int(config['gatk-annotate']['num_cores']),
                                          memory="{}G".format(config['gatk-annotate']['max_mem']))

        gatk_filter_job = Job.wrapJobFn(gatk.filter_variants, config, sample, gatk_annotate_job.rv(),
                                        cores=1,
                                        memory="{}G".format(config['gatk-filter']['max_mem']))

        snpeff_job = Job.wrapJobFn(annotation.snpeff, config, sample, "{}.filtered.vcf".format(sample),
                                   cores=int(config['snpeff']['num_cores']),
                                   memory="{}G".format(config['snpeff']['max_mem']))

        vcfanno_job = Job.wrapJobFn(annotation.vcfanno, config, sample, samples,
                                    "{}.snpEff.{}.vcf".format(sample, config['snpeff']['reference']),
                                    cores=int(config['vcfanno']['num_cores']),
                                    memory="{}G".format(config['vcfanno']['max_mem']))

        # Create workflow from created jobs
        root_job.addChild(spawn_filter_job)

        spawn_filter_job.addChild(bgzip_tabix_job1)
        bgzip_tabix_job1.addChild(add_contig_job1)
        add_contig_job1.addChild(qual_filter_job1)

        spawn_filter_job.addChild(bgzip_tabix_job2)
        bgzip_tabix_job2.addChild(add_contig_job2)
        add_contig_job2.addChild(qual_filter_job2)

        spawn_filter_job.addChild(bgzip_tabix_job3)
        bgzip_tabix_job3.addChild(add_contig_job3)
        add_contig_job3.addChild(qual_filter_job3)

        spawn_filter_job.addChild(bgzip_tabix_job4)
        bgzip_tabix_job4.addChild(add_contig_job4)
        add_contig_job4.addChild(qual_filter_job4)

        spawn_filter_job.addChild(bgzip_tabix_job5)
        bgzip_tabix_job5.addChild(add_contig_job5)
        add_contig_job5.addChild(qual_filter_job5)

        spawn_filter_job.addChild(bgzip_tabix_job6)
        bgzip_tabix_job6.addChild(add_contig_job6)
        add_contig_job6.addChild(qual_filter_job6)

        spawn_filter_job.addFollowOn(merge_job)

        merge_job.addChild(gatk_annotate_job)
        gatk_annotate_job.addChild(gatk_filter_job)
        gatk_filter_job.addChild(snpeff_job)
        snpeff_job.addChild(vcfanno_job)

    root_job.addFollowOn(fastqc_job)
    # Start workflow execution
    Job.Runner.startToil(root_job, args)
