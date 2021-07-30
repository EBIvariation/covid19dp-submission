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
import inspect
import os
import sys
import tarfile
import urllib.request

import yaml

from covid19dp_submission import NEXTFLOW_DIR
from .steps.vcf_vertical_concat.run_vcf_vertical_concat_pipeline import get_concat_result_file_name
from ebi_eva_common_pyutils.config_utils import get_args_from_private_config_file
from ebi_eva_common_pyutils.command_utils import run_command_with_output
from typing import List


def get_submission_snapshot_file_list(download_url: str) -> List[str]:
    ftp_stream = urllib.request.urlopen(download_url)
    tar_file_handle = tarfile.open(fileobj=ftp_stream, mode="r|gz")
    return sorted([os.path.basename(member.name) for member in tar_file_handle.getmembers()
                   if member.name.lower().endswith(".vcf.gz")])


def _create_required_dirs(config: dict):
    required_dirs = [config['submission']['download_target_dir'], config['submission']['concat_processing_dir'],
                     config['submission']['accession_output_dir'], config['submission']['ftp_project_dir'],
                     config['submission']['log_dir'], config['submission']['validation_dir']]
    for dir_name in required_dirs:
        os.makedirs(dir_name, exist_ok=True)


def _create_download_file_list(config: dict):
    download_file_list = get_submission_snapshot_file_list(config['submission']['download_url'])
    open(config['submission']['download_file_list'], "w").write('\n'.join(download_file_list))
    return download_file_list


def _get_config(download_url: str, snapshot_name: str, project_dir: str, nextflow_config_file: str,
                app_config_file: str) -> dict:
    config = get_args_from_private_config_file(app_config_file)

    download_target_dir = os.path.join(project_dir, '30_eva_valid', snapshot_name)
    download_file_list = os.path.join(download_target_dir, 'file_list.csv')
    submission_param_file = os.path.join(download_target_dir, 'nf_params.yml')
    concat_processing_dir = os.path.join(download_target_dir, 'processed')
    log_dir = os.path.join(project_dir, '00_logs', snapshot_name)
    validation_dir = os.path.join(log_dir, 'validation')
    accession_output_dir = os.path.join(project_dir, '60_eva_public', snapshot_name)

    config['submission'].update(
               {'download_url': download_url, 'snapshot_name': snapshot_name,
                'download_target_dir': download_target_dir, 'download_file_list': download_file_list,
                # Directory to process vertical concatenation of submitted VCF files
                'concat_processing_dir': concat_processing_dir,
                'accession_output_dir': accession_output_dir,
                'accession_output_file': os.path.join(accession_output_dir, f'{snapshot_name}.accessioned.vcf'),
                'log_dir': log_dir, 'validation_dir': validation_dir
                })
    config['app']['python'] = {'interpreter': sys.executable,
                               'script_path': os.path.dirname(inspect.getmodule(sys.modules[__name__]).__file__)}
    config['app']['nextflow_config_file'] = nextflow_config_file
    config['app']['nextflow_param_file'] = submission_param_file

    return config


def ingest_covid19dp_submission(download_url: str, snapshot_name: str or None,  project_dir: str,
                                app_config_file: str, nextflow_config_file: str or None, resume: bool):
    snapshot_name = snapshot_name if snapshot_name else os.path.basename(download_url).replace(".tar.gz", "")
    config = _get_config(download_url, snapshot_name, project_dir, nextflow_config_file, app_config_file)
    _create_required_dirs(config)
    # Compute the file path where the output of multi-stage vertical concatenation will reside
    # based on the number of files to be processed
    vcf_files_to_be_downloaded = _create_download_file_list(config)
    config['submission']['concat_result_file'] = \
        get_concat_result_file_name(config['submission']['concat_processing_dir'], len(vcf_files_to_be_downloaded),
                                    config['submission']['concat_chunk_size'])

    nextflow_file_to_run = os.path.join(NEXTFLOW_DIR, 'submission_workflow.nf')
    yaml.safe_dump(config, open(config['app']['nextflow_param_file'], "w"))

    run_nextflow_command = f"{config['app']['nextflow_binary']} run {nextflow_file_to_run}"
    run_nextflow_command += f" -c {nextflow_config_file}" if nextflow_config_file else ""
    run_nextflow_command += f" -resume" if resume else ""
    run_nextflow_command += f" --PYTHONPATH {config['app']['python']['script_path']}"
    run_nextflow_command += f" -params-file {config['app']['nextflow_param_file']}"
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
