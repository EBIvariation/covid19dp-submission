# Covid-19 Data Portal Submission Automation

This repository contains automation scripts to process submissions from the Covid-19 data portal project: https://www.covid19dataportal.org/

## Installation

Retrieve the latest version
```
pip git+install https://github.com/EBIvariation/covid19dp-submission.git@master
```

Retrieve a tagged version
```
pip git+install https://github.com/EBIvariation/covid19dp-submission.git@v0.1.2
```

## Usage

``` 
ingest_covid19dp_submission.py --download-url http://path/to/covid19dp/snapshot/YYYY_MM_DD_filtered_vcf.tar.gz --project-dir /path/to/project/dir --app-config-file /path/to/app_config.yml --nextflow-config-file /path/to/nextflow.config
```

See [application configuration](covid19dp_submission/etc/example_app_config.yml) and [nextflow configuration](covid19dp_submission/etc/example_nextflow.config) examples. 

The above command will run the following steps (see [workflow definition](covid19dp_submission/nextflow/submission_workflow.nf)):

* [Download the snapshot](covid19dp_submission/steps/download_snapshot.py) from the URL provided for a given Covid-19 DP snapshot.
* Run [VCF validation](covid19dp_submission/steps/run_vcf_validator.py) on all the downloaded VCF files.
* Run [bgzip compression and indexing](covid19dp_submission/steps/bgzip_and_index_vcf.py) on the VCF files. 
* Run [multi-stage vertical concatenation](covid19dp_submission/steps/vcf_vertical_concat/run_vcf_vertical_concat_pipeline.py) to combine the VCF files.
* [Accession](covid19dp_submission/steps/accession_vcf.py) the resulting combined VCF file from the step above.
* Publish the accessioned files to the Covid-19 DP project directory in the public FTP.
* [Cluster the variants in the SARS-Cov-2](covid19dp_submission/steps/cluster_assembly.py) assembly in the accessioning warehouse.

For usage in EBI cluster, see [here](https://www.ebi.ac.uk/panda/jira/browse/EVA-2495?focusedCommentId=366472&page=com.atlassian.jira.plugin.system.issuetabpanels:comment-tabpanel#comment-366472) (limited to EBI internal users only).
