nextflow.enable.dsl=2

params.NORMALISED_VCF_DIR = "${params.submission.download_target_dir}/normalised_vcfs"
params.REFSEQ_FASTA = "${params.submission.download_target_dir}/refseq_fasta.fa"

// This is needed because "bcftools norm" step requires a FASTA
// but the VCFs we get from Covid19 data team only have RefSeq contigs
process create_refseq_fasta {
    output:
    val true, emit: create_refseq_fasta_success

    script:
    // TODO: Doing this seems like a bad idea but this seems to work well for now
    """
    sed s/MN908947.3/NC_045512.2/g $params.submission.assembly_fasta > $params.REFSEQ_FASTA
    """
}

process validate_vcfs {

    input:
    path vcf_files

    output:
    val true, emit: validate_vcfs_success

    script:
    """
    export PYTHONPATH="$params.executable.python.script_path"
    ($params.executable.python.interpreter \
        -m steps.run_vcf_validator \
        --vcf-file  $vcf_files \
        --validator-binary $params.executable.vcf_validator \
        --output-dir $params.submission.validation_dir \
    ) >> $params.submission.log_dir/validate_vcfs.log 2>&1
    """
}

process asm_check_vcfs {

    input:
    path vcf_files

    output:
    val true, emit: asm_check_vcfs_success

    script:
    """
    export PYTHONPATH="$params.executable.python.script_path"
    ($params.executable.python.interpreter \
        -m steps.run_asm_checker \
        --vcf-file  $vcf_files \
        --assembly-checker-binary $params.executable.vcf_assembly_checker \
        --assembly-report $params.submission.assembly_report \
        --assembly-fasta $params.submission.assembly_fasta \
        --output-dir $params.submission.validation_dir \
    ) >> $params.submission.log_dir/asm_check_vcfs.log 2>&1
    """
}

process bgzip_and_index {

    input:
    val flag1
    val flag2
    path vcf_files

    output:
    val true, emit: bgzip_and_index_success

    script:
    """
    export PYTHONPATH="$params.executable.python.script_path"
    ($params.executable.python.interpreter \
        -m steps.bgzip_and_index_vcf \
        --vcf-file  $vcf_files \
        --output-dir $params.submission.download_target_dir \
        --bcftools-binary $params.executable.bcftools \
    ) >> $params.submission.log_dir/bgzip_and_index_vcfs.log 2>&1
    """
}

process vertical_concat {
    input:
    val flag

    output:
    val true, emit: vertical_concat_success

    script:
    """
    export PYTHONPATH="$params.executable.python.script_path"
    ($params.executable.python.interpreter \
        -m steps.vcf_vertical_concat.run_vcf_vertical_concat_pipeline \
        --toplevel-vcf-dir $params.NORMALISED_VCF_DIR \
        --concat-processing-dir $params.submission.concat_processing_dir \
        --concat-chunk-size $params.submission.concat_chunk_size \
        --bcftools-binary $params.executable.bcftools \
        --nextflow-binary $params.executable.nextflow \
        --nextflow-config-file $params.executable.nextflow_config_file \
    ) >> $params.submission.log_dir/vertical_concat.log 2>&1
    """
}

process normalise_concat_vcf {

    input:
    val flag1

    output:
    val true, emit: normalise_concat_vcf_success

    script:
    """
    export PYTHONPATH="$params.executable.python.script_path"
    ($params.executable.python.interpreter \
        -m steps.normalise_vcfs \
        --vcf-files  $params.submission.concat_result_file \
        --input-dir `dirname ${params.submission.concat_result_file}` \
        --output-dir $params.NORMALISED_VCF_DIR \
        --bcftools-binary $params.executable.bcftools \
        --refseq-fasta-file $params.REFSEQ_FASTA \
    ) >> $params.submission.log_dir/normalise_concat_vcf.log 2>&1
    """
}

process accession_vcf {
    clusterOptions "-g /accession/$params.submission.accessioning_instance"

    input:
    val flag

    output:
    val true, emit: accession_vcf_success

    script:
    //Accessioning properties file passed via command line should already be populated with project and assembly accessions
    """
    export PYTHONPATH="$params.executable.python.script_path"
    export NORMALISED_CONCAT_VCF=("${params.NORMALISED_VCF_DIR}/"`basename ${params.submission.concat_result_file}`)
    ($params.executable.python.interpreter \
        -m steps.accession_vcf \
        --vcf-file \$NORMALISED_CONCAT_VCF \
        --accessioning-jar-file $params.jar.accession_pipeline \
        --accessioning-properties-file $params.submission.accessioning_properties_file \
        --accessioning-instance $params.submission.accessioning_instance \
        --output-vcf-file $params.submission.accession_output_file \
        --bcftools-binary $params.executable.bcftools \
    )  >> $params.submission.log_dir/accession_vcf.log 2>&1
    """
}

process sync_accessions_to_public_ftp {
    label 'datamover'

    input:
    val flag

    output:
    val true, emit: sync_accessions_to_public_ftp_success

    script:
    """
    mkdir -p $params.submission.public_ftp_dir
    (rsync -av $params.submission.accession_output_dir/* $params.submission.public_ftp_dir) \
    >> $params.submission.log_dir/sync_accessions_to_public_ftp.log 2>&1
    """
}

process cluster_assembly {
    clusterOptions "-g /accession/$params.submission.clustering_instance"

    input:
    val flag

    output:
    val true, emit: cluster_assembly_success

    script:
    //Clustering properties file passed via command line should already be populated with project and assembly accessions
    """
    export PYTHONPATH="$params.executable.python.script_path"
    ($params.executable.python.interpreter \
        -m steps.cluster_assembly \
        --clustering-jar-file $params.jar.clustering_pipeline \
        --clustering-properties-file $params.submission.clustering_properties_file \
        --accessioning-instance $params.submission.clustering_instance \
    )  >> $params.submission.log_dir/cluster_assembly.log 2>&1
    """
}

process incremental_release {

    input:
    val flag

    output:
    val true, emit: incremental_release_success

    // Due to the particularly memory-intensive nature of the incremental release process (owing to in-memory joins),
    // ensure that this number is at least 32 in real-world scenarios
    memory "${params.submission.memory_for_incremental_release_in_gb}" + " GB"

    script:
    """
    (java -Xmx${params.submission.memory_for_incremental_release_in_gb}g -jar $params.jar.release_pipeline \
    --spring.config.location=$params.submission.release_properties_file \
    --parameters.accessionedVcf="${params.submission.accession_output_file}.gz"
    )  >> $params.submission.log_dir/incremental_release.log 2>&1
    """
}

workflow  {
    main:
	    create_refseq_fasta()
        Channel.fromPath("$params.submission.download_file_list")
               .splitCsv(header:false)
               .map(row -> row[0])
               .buffer( size:params.submission.batch_size, remainder: true )
               .set{vcf_files_list}
        validate_vcfs(vcf_files_list)
        asm_check_vcfs(vcf_files_list)
        bgzip_and_index(validate_vcfs.out.validate_vcfs_success, asm_check_vcfs.out.asm_check_vcfs_success, vcf_files_list)
        vertical_concat(bgzip_and_index.out.bgzip_and_index_success.collect()) | \
        normalise_concat_vcf | accession_vcf | sync_accessions_to_public_ftp | cluster_assembly | incremental_release
}
