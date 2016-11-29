CLINICAL_GEMINI_QUERY_BASE = "select v.chrom, v.start, v.end, v.vcf_id, v.type, v.sub_type, v.ref, v.alt, v.filter, " \
                             "v.gene, v.biotype, v.impact, v.impact_so, v.impact_severity, v.codon_change, " \
                             "v.transcript, v.aa_change, v.exon, v.in_omim, v.clinvar_sig, v.clinvar_disease_name, " \
                             "v.clinvar_dbsource, v.clinvar_dbsource_id, v.clinvar_origin, v.clinvar_dsdb, " \
                             "v.clinvar_dsdbid, v.clinvar_disease_acc, v.clinvar_in_locus_spec_db, " \
                             "v.clinvar_on_diag_assay, v.rs_ids, v.max_aaf_all, v.aaf_esp_ea, v.aaf_esp_aa, " \
                             "v.aaf_esp_all, v.aaf_1kg_eur, v.aaf_1kg_amr, v.aaf_1kg_asn, v.aaf_1kg_afr, " \
                             "v.aaf_1kg_all, v.is_conserved, v.is_somatic, g.ensembl_gene_id