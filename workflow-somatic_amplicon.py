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

    # Per sample jobs
    for sample in samples:
        # Alignment and Refinement Stages
        align_job = Job.wrapJobFn(bwa.run_bwa_mem, config, sample, samples,
                                  cores=int(config['bwa']['num_cores']),
                                  memory="{}G".format(config['bwa']['max_mem']))

        add_job = Job.wrapJobFn(gatk.add_or_replace_readgroups, config, sample, align_job.rv(),
                                cores=1,
                                memory="{}G".format(config['gatk']['max_mem']))

        creator_job = Job.wrapJobFn(gatk.realign_target_creator, config, sample, add_job.rv(),
                                    cores=int(config['gatk']['num_cores']),
                                    memory="{}G".format(config['gatk']['max_mem']))

        realign_job = Job.wrapJobFn(gatk.realign_indels, config, sample, add_job.rv(), creator_job.rv(),
                                    cores=1,
                                    memory="{}G".format(config['gatk']['max_mem']))

        recal_job = Job.wrapJobFn(gatk.recalibrator, config, sample, realign_job.rv(),
                                  cores=int(config['gatk']['num_cores']),
                                  memory="{}G".format(config['gatk']['max_mem']))
        # Variant Calling
        spawn_variant_job = Job.wrapJobFn(pipeline.spawn_variant_jobs)
        freebayes_job = Job.wrapJobFn(freebayes.freebayes_single, config, sample, recal_job.rv(),
                                      cores=1,
                                      memory="{}G".format(config['freebayes']['max_mem']))

        mutect_job = Job.wrapJobFn(mutect.mutect_single, config, sample, samples, recal_job.rv(),
                                   cores=1,
                                   memory="{}G".format(config['mutect']['max_mem']))

        # mutect2_job = Job.wrapJobFn(mutect.mutect2_single, config, sample, samples, recal_job.rv(),
        #                             cores="{}G".format(config['gatk3.5']['max_mem'],
        #                             memory="{}G".format(config['gatk3.5']['max_mem']))
        #
        vardict_job = Job.wrapJobFn(vardict.vardict_single, config, sample, samples, recal_job.rv(),
                                    cores=int(config['vardict']['num_cores']),
                                    memory="{}G".format(config['vardict']['max_mem']))

        scalpel_job = Job.wrapJobFn(scalpel.scalpel_single, config, sample, samples, recal_job.rv(),
                                    cores=int(config['scalpel']['num_cores']),
                                    memory="{}G".format(config['scalpel']['max_mem']))

        # scanindel_job = Job.wrapJobFn(scanindel.scanindel, config, sample, samples, recal_job.rv(),
        #                               cores=int(config['scanindel']['num_cores']),
        #                               memory="{}G".format(config['scanindel']['max_mem']))

        platypus_job = Job.wrapJobFn(platypus.platypus_single, config, sample, samples, recal_job.rv(),
                                     cores=int(config['platypus']['num_cores']),
                                     memory="{}G".format(config['platypus']['max_mem']))

        pindel_job = Job.wrapJobFn(pindel.run_pindel, config, sample, recal_job.rv(),
                                   cores=int(config['pindel']['num_cores']),
                                   memory="{}G".format(config['pindel']['max_mem']))

        # Need to filter for on target only results somewhere as well
        spawn_normalization_job = Job.wrapJobFn(pipeline.spawn_variant_jobs)

        normalization_job1 = Job.wrapJobFn(variation.vt_normalization, config, sample, "freebayes",
                                           freebayes_job.rv(),
                                           cores=1,
                                           memory="{}G".format(config['gatk']['max_mem']))

        normalization_job2 = Job.wrapJobFn(variation.vt_normalization, config, sample, "mutect",
                                           mutect_job.rv(),
                                           cores=1,
                                           memory="{}G".format(config['gatk']['max_mem']))

        normalization_job3 = Job.wrapJobFn(variation.vt_normalization, config, sample, "vardict",
                                           vardict_job.rv(),
                                           cores=1,
                                           memory="{}G".format(config['gatk']['max_mem']))

        normalization_job4 = Job.wrapJobFn(variation.vt_normalization, config, sample, "scalpel",
                                           scalpel_job.rv(),
                                           cores=1,
                                           memory="{}G".format(config['gatk']['max_mem']))

        normalization_job5 = Job.wrapJobFn(variation.vt_normalization, config, sample, "platypus",
                                           platypus_job.rv(),
                                           cores=1,
                                           memory="{}G".format(config['gatk']['max_mem']))

        normalization_job6 = Job.wrapJobFn(variation.vt_normalization, config, sample, "pindel",
                                           # pindel_job.rv(),
                                           "{}.pindel.vcf".format(sample),
                                           cores=1,
                                           memory="{}G".format(config['gatk']['max_mem']))

        # normalization_job5 = Job.wrapJobFn(variation.vt_normalization, config, sample, "scanindel",
        #                                    scanindel_job.rv(),
        #                                    cores=1,
        #                                    memory="{}G".format(config['gatk']['max_mem']))

        callers = "freebayes, mutect, vardict, scalpel, platypus, pindel"

        merge_job = Job.wrapJobFn(variation.merge_variant_calls, config, sample, callers, (normalization_job1.rv(),
                                                                                           normalization_job2.rv(),
                                                                                           normalization_job3.rv(),
                                                                                           normalization_job4.rv(),
                                                                                           normalization_job5.rv(),
                                                                                           normalization_job6.rv()))

        gatk_annotate_job = Job.wrapJobFn(gatk.annotate_vcf, config, sample, merge_job.rv(), recal_job.rv(),
                                          cores=int(config['gatk']['num_cores']),
                                          memory="{}G".format(config['gatk']['max_mem']))

        gatk_filter_job = Job.wrapJobFn(gatk.filter_variants, config, sample, gatk_annotate_job.rv(),
                                        cores=1,
                                        memory="{}G".format(config['gatk']['max_mem']))

        snpeff_job = Job.wrapJobFn(annotation.snpeff, config, sample, gatk_filter_job.rv(),
                                   cores=int(config['snpeff']['num_cores']),
                                   memory="{}G".format(config['snpeff']['max_mem']))

        vcfanno_job = Job.wrapJobFn(annotation.vcfanno, config, sample, snpeff_job.rv(),
                                    cores=int(config['vcfanno']['num_cores']),
                                    memory="{}G".format(config['vcfanno']['max_mem']))

        # Create workflow from created jobs
        root_job.addChild(align_job)
        align_job.addChild(add_job)
        add_job.addChild(creator_job)
        creator_job.addChild(realign_job)
        realign_job.addChild(recal_job)

        recal_job.addChild(spawn_variant_job)

        spawn_variant_job.addChild(freebayes_job)
        spawn_variant_job.addChild(mutect_job)
        # spawn_variant_job.addChild(mutect2_job)
        spawn_variant_job.addChild(vardict_job)
        spawn_variant_job.addChild(scalpel_job)
        # spawn_variant_job.addChild(scanindel_job)
        spawn_variant_job.addChild(platypus_job)
        spawn_variant_job.addChild(pindel_job)

        spawn_variant_job.addFollowOn(spawn_normalization_job)

        spawn_normalization_job.addChild(normalization_job1)
        spawn_normalization_job.addChild(normalization_job2)
        spawn_normalization_job.addChild(normalization_job3)
        spawn_normalization_job.addChild(normalization_job4)
        spawn_normalization_job.addChild(normalization_job5)
        spawn_normalization_job.addChild(normalization_job6)

        spawn_normalization_job.addFollowOn(merge_job)

        merge_job.addChild(gatk_annotate_job)
        # on_target_job.addChild(gatk_annotate_job)
        gatk_annotate_job.addChild(gatk_filter_job)
        gatk_filter_job.addChild(snpeff_job)
        snpeff_job.addChild(vcfanno_job)

    # Start workflow execution
    Job.Runner.startToil(root_job, args)
