#!/usr/bin/env python

# Standard packages
import sys
import cyvcf2
import argparse
import geneimpacts

from cyvcf2 import VCF


def get_effects(variant, annotation_keys):
    effects = []
    effects += [geneimpacts.SnpEff(e, annotation_keys) for e in variant.INFO.get("ANN").split(",")]

    return effects


def get_top_impact(effects):
    top_impact = geneimpacts.Effect.top_severity(effects)

    if isinstance(top_impact, list):
        top_impact = top_impact[0]

    return top_impact


def get_genes(effects):
    genes_list = []

    for effect in effects:
        if effect.gene not in genes_list:
            genes_list.append(effect.gene)

    return genes_list


def get_transcript_effects(effects):
    transcript_effects = dict()

    for effect in effects:
        if effect.transcript is not None:
            transcript_effects[effect.transcript] = "{biotype}|{effect}".format(biotype=effect.biotype,
                                                                                effect=effect.impact_severity)

    return transcript_effects


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-a', '--annotated_vcf', help="snpEff annotated VCF file to scan")
    parser.add_argument('-o', '--output', help="File for output information")
    args = parser.parse_args()

    sys.stdout.write("Parsing VCFAnno VCF with CyVCF2\n")
    reader = cyvcf2.VCFReader(args.annotated_vcf)
    desc = reader["ANN"]["Description"]
    annotation_keys = [x.strip("\"'") for x in re.split("\s*\|\s*", desc.split(":", 1)[1].strip('" '))]

    sys.stdout.write("Parsing VCFAnno VCF\n")
    vcf = VCF(args.annotated_vcf)
    for variant in vcf:
        effects = get_effects(variant, annotation_keys)
        top_impact = get_top_impact(effects)
        gene_effects = dict()
        for effect in effects:
            if effect.gene not in gene_effects.keys():
                if effect.transcript is not None:
