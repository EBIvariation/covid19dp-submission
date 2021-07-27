process download_snapshot {{
input:
val flag from true
output:
val true into download_snapshot_success
shell:
'''
(export PYTHONPATH="{app[python][script_path]}" && {app[python][interpreter]} -m steps.download_snapshot --download-url {submission[download_url]} --snapshot-name {submission[snapshot_name]} --download-target-dir {submission[download_target_dir]}) >> {submission[log_dir]}/download_snapshot.log 2>&1
'''
}}

process validate_vcfs {{
Channel.fromList({submission[download_files]}).set{{vcf_file_list}}
input:
val flag from download_snapshot_success
val vcf_file from vcf_file_list
output:
val true into validate_vcfs_success
shell:
'''
(export PYTHONPATH="{app[python][script_path]}" && {app[python][interpreter]} -m steps.run_vcf_validator --vcf-file {submission[download_target_dir]}/!{{vcf_file}} --validator-binary {app[validator_binary]} --output-dir {submission[validation_dir]}) >> {submission[log_dir]}/validate_vcfs.log 2>&1
'''
}}

process bgzip_and_index {{
Channel.fromList({submission[download_files]}).set{{vcf_file_list}}
input:
val flag from validate_vcfs_success.collect()
val vcf_file from vcf_file_list
output:
val true into bgzip_and_index_success
shell:
'''
(export PYTHONPATH="{app[python][script_path]}" && {app[python][interpreter]} -m steps.bgzip_and_index_vcf --vcf-file {submission[download_target_dir]}/!{{vcf_file}} --bcftools-binary {app[bcftools_binary]}) >> {submission[log_dir]}/bgzip_and_index_vcfs.log 2>&1
'''
}}

process vertical_concat {{
input:
val flag from bgzip_and_index_success.collect()
output:
val true into vertical_concat_success
shell:
'''
(export PYTHONPATH="{app[python][script_path]}" && {app[python][interpreter]} -m steps.vcf_vertical_concat.run_vcf_vertical_concat_pipeline --toplevel-vcf-dir {submission[download_target_dir]} --concat-processing-dir {submission[concat_processing_dir]} --concat-chunk-size {submission[concat_chunk_size]} --bcftools-binary {app[bcftools_binary]} --nextflow-binary {app[nextflow_binary]} --nextflow-config-file {app[nextflow_config_file]}) >> {submission[log_dir]}/vertical_concat.log 2>&1
'''
}}

process accession_vcf {{
input:
val flag from vertical_concat_success
output:
val true into accession_vcf_success
shell:
'''
(export PYTHONPATH="{app[python][script_path]}" && {app[python][interpreter]} -m steps.accession_vcf --vcf-file {submission[concat_result_file]} --accessioning-jar-file {app[accessioning_jar_file]} --accessioning-properties-file {app[accessioning_properties_file]} --output-vcf-file {submission[accession_output_file]} --bcftools-binary {app[bcftools_binary]})  >> {submission[log_dir]}/accession_vcf.log 2>&1
'''
}}

process cluster_assembly {{
input:
val flag from accession_vcf_success
output:
val true into cluster_assembly_success
shell:
'''
(export PYTHONPATH="{app[python][script_path]}" && {app[python][interpreter]} -m steps.cluster_assembly --clustering-jar-file {app[clustering_jar_file]} --clustering-properties-file {app[clustering_properties_file]})  >> {submission[log_dir]}/cluster_assembly.log 2>&1
'''
}}
