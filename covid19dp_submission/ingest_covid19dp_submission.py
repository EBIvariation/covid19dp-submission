# Copyright 2021 EMBL - European Bioinformatics Institute
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import argparse
import covid19dp_submission
import inspect
import os
import sys
import tarfile
import urllib.request

from covid19dp_submission import NEXTFLOW_DIR
from covid19dp_submission.steps.vcf_vertical_concat.run_vcf_vertical_concat_pipeline import get_concat_result_file_name
from ebi_eva_common_pyutils.config_utils import get_args_from_private_config_file
from ebi_eva_common_pyutils.command_utils import run_command_with_output
from typing import List


def get_submission_snapshot_file_list(download_url: str) -> List[str]:
    ftp_stream = urllib.request.urlopen(download_url)
    tar_file_handle = tarfile.open(fileobj=ftp_stream, mode="r|gz")
    return sorted([os.path.basename(member.name) for member in tar_file_handle.getmembers()
                   if member.name.lower().endswith(".vcf.gz")])


def _get_config(download_url: str, snapshot_name: str, project_dir: str, nextflow_config_file: str,
                app_config_file: str) -> dict:
    config = get_args_from_private_config_file(app_config_file)
    download_target_dir = os.path.join(project_dir, '30_eva_valid', snapshot_name)
    os.makedirs(download_target_dir, exist_ok=True)
    os.makedirs(config['submission']['ftp_project_dir'], exist_ok=True)
    download_files = get_submission_snapshot_file_list(download_url)
    download_file_list = os.path.join(download_target_dir, 'file_list.csv')
    open(download_file_list, "w").write('\n'.join(download_files))
    concat_processing_dir = os.path.join(download_target_dir, 'processed')

    log_dir = os.path.join(project_dir, '00_logs', snapshot_name)
    os.makedirs(log_dir, exist_ok=True)
    validation_dir = os.path.join(log_dir, 'validation')
    os.makedirs(validation_dir, exist_ok=True)
    accession_output_dir = os.path.join(project_dir, '60_eva_public', snapshot_name)
    os.makedirs(accession_output_dir, exist_ok=True)

    config['submission'].update(
               {'download_url': download_url, 'snapshot_name': snapshot_name,
                'download_target_dir': download_target_dir, 'download_file_list': download_file_list,
                # Directory to process vertical concatenation of submitted VCF files
                'concat_processing_dir': concat_processing_dir,
                'concat_result_file': get_concat_result_file_name(concat_processing_dir, len(download_files),
                                                                  config['submission']['concat_chunk_size']),
                'accession_output_dir': accession_output_dir,
                'accession_output_file': os.path.join(accession_output_dir, f'{snapshot_name}.accessioned.vcf'),
                'log_dir': log_dir, 'validation_dir': validation_dir
                })
    config['app']['python'] = {'interpreter': sys.executable,
                               'script_path': os.path.dirname(inspect.getmodule(covid19dp_submission).__file__)}
    config['app']['nextflow_config_file'] = nextflow_config_file
    return config


def ingest_covid19dp_submission(download_url: str, snapshot_name: str or None,  project_dir: str,
                                app_config_file: str, nextflow_config_file: str or None, resume: bool):
    snapshot_name = snapshot_name if snapshot_name else os.path.basename(download_url).replace(".tar.gz", "")
    config = _get_config(download_url, snapshot_name, project_dir, nextflow_config_file, app_config_file)

    nextflow_template_file = os.path.join(NEXTFLOW_DIR, 'submission_workflow_template.nf')
    nextflow_template = open(nextflow_template_file).read()
    nextflow_file_to_run = os.path.join(os.getcwd(), f"submission_{snapshot_name}.nf")
    open(nextflow_file_to_run, "w").write(nextflow_template.format(**config))

    run_nextflow_command = f"{config['app']['nextflow_binary']} run {nextflow_file_to_run}"
    run_nextflow_command += f" -c {nextflow_config_file}" if nextflow_config_file else ""
    run_nextflow_command += f" -resume" if resume else ""
    run_command_with_output(f"Running submission pipeline: {nextflow_file_to_run}...", run_nextflow_command)


def main():
    parser = argparse.ArgumentParser(description='Ingest a snapshot submission from the Covid-19 data portal project',
                                     formatter_class=argparse.RawTextHelpFormatter, add_help=False)
    parser.add_argument("--download-url",
                        help="URL to the data snapshot (ex: http://path/to/snapshots/YYYY_MM_DD.tar.gz)", required=True)
    parser.add_argument("--snapshot-name", help="Snapshot name (ex: 2021_06_28_filtered_vcf)", default=None,
                        required=False)
    parser.add_argument("--project-dir", help="Project directory (ex: /path/to/PRJ)", default=None, required=True)
    parser.add_argument("--app-config-file",
                        help="Full path to the application config file (ex: /path/to/config.yml)", required=True)
    parser.add_argument("--nextflow-config-file",
                        help="Full path to the Nextflow config file", default=None, required=False)
    parser.add_argument("--resume",
                        help="Indicate if a previous concatenation job is to be resumed", action='store_true',
                        required=False)
    args = parser.parse_args()
    ingest_covid19dp_submission(args.download_url, args.snapshot_name, args.project_dir, args.app_config_file,
                                args.nextflow_config_file, args.resume)


if __name__ == "__main__":
    main()
