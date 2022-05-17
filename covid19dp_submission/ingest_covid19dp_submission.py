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

import inspect
import os
import sys
from datetime import datetime
from typing import List

import yaml
from ebi_eva_common_pyutils.command_utils import run_command_with_output
from ebi_eva_common_pyutils.config_utils import get_args_from_private_config_file
from ebi_eva_common_pyutils.logger import logging_config

from covid19dp_submission import NEXTFLOW_DIR
from covid19dp_submission.download_analyses import download_analyses
from covid19dp_submission.steps.vcf_vertical_concat.run_vcf_vertical_concat_pipeline import get_concat_result_file_name


logger = logging_config.get_logger(__name__)


def get_analyses_file_list(download_target_dir: str) -> List[str]:
    return sorted([os.path.basename(member) for member in os.listdir(download_target_dir)
                   if member.lower().endswith(".vcf") or member.lower().endswith(".vcf.gz")])


def _create_required_dirs(config: dict):
    required_dirs = [config['submission']['download_target_dir'],
                     config['submission']['concat_processing_dir'],
                     config['submission']['accession_output_dir'],
                     config['submission']['log_dir'],
                     config['submission']['validation_dir']]
    for dir_name in required_dirs:
        os.makedirs(dir_name, exist_ok=True)


def create_download_file_list(config: dict):
    download_file_list = get_analyses_file_list(config['submission']['download_target_dir'])
    open(config['submission']['download_file_list'], "w").write('\n'.join(download_file_list))
    return download_file_list


def _get_config(snapshot_name: str, project_dir: str, nextflow_config_file: str, app_config_file: str) -> dict:
    config = get_args_from_private_config_file(app_config_file)

    download_target_dir = os.path.join(project_dir, '30_eva_valid', snapshot_name)
    download_file_list = os.path.join(download_target_dir, 'file_list.csv')
    submission_param_file = os.path.join(download_target_dir, 'nf_params.yml')
    concat_processing_dir = os.path.join(download_target_dir, 'processed')
    log_dir = os.path.join(project_dir, '00_logs', snapshot_name)
    validation_dir = os.path.join(log_dir, 'validation')
    accession_output_dir = os.path.join(project_dir, '60_eva_public', snapshot_name)

    config['submission'].update(
        {'snapshot_name': snapshot_name,
         'download_target_dir': download_target_dir, 'download_file_list': download_file_list,
         # Directory to process vertical concatenation of submitted VCF files
         'concat_processing_dir': concat_processing_dir,
         'accession_output_dir': accession_output_dir,
         'accession_output_file': os.path.join(accession_output_dir, f'{snapshot_name}.accessioned.vcf'),
         'log_dir': log_dir, 'validation_dir': validation_dir
         })
    config['executable']['python'] = {'interpreter': sys.executable,
                                      'script_path': os.path.dirname(inspect.getmodule(sys.modules[__name__]).__file__)}
    config['executable']['nextflow_config_file'] = nextflow_config_file
    config['executable']['nextflow_param_file'] = submission_param_file
    return config


def ingest_covid19dp_submission(project: str, project_dir: str, num_analyses: int,
                                processed_analyses_file: str, app_config_file: str, nextflow_config_file: str or None,
                                resume: str):
    process_new_snapshot = False
    if resume is None:
        snapshot_name = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
        process_new_snapshot = True
    else:
        snapshot_name = resume

    config = _get_config(snapshot_name, project_dir, nextflow_config_file, app_config_file)

    if process_new_snapshot:
        _create_required_dirs(config)
    else:
        # Check that the snapshot exists
        assert os.path.exists(
            config['submission']['download_target_dir']), f'Cannot resume execution for snapshot {snapshot_name}'

    list_file = get_analyses_file_list(config['submission']['download_target_dir'])
    if len(list_file) < num_analyses:
        num_analyses = num_analyses - len(list_file)
        download_analyses(project, num_analyses, processed_analyses_file, config['submission']['download_target_dir'],
                          config['executable']['ascp_bin'], config['aspera']['aspera_id_dsa_key'], config.get('download_batch_size', 100))
    else:
        logger.info(f'All {num_analyses} analysis have been downloaded already. Skipping.')
    vcf_files_to_be_downloaded = create_download_file_list(config)
    config['submission']['concat_result_file'] = \
        get_concat_result_file_name(config['submission']['concat_processing_dir'], len(vcf_files_to_be_downloaded),
                                    config['submission']['concat_chunk_size'])

    nextflow_file_to_run = os.path.join(NEXTFLOW_DIR, 'submission_workflow.nf')
    yaml.safe_dump(config, open(config['executable']['nextflow_param_file'], "w"))

    # run the nextflow script in the download directory so that each execution is independent
    run_nextflow_command = (f"cd {config['submission']['download_target_dir']}; " 
                            f"{config['executable']['nextflow']} run {nextflow_file_to_run} "
                            f"-c {nextflow_config_file} " 
                            f"--PYTHONPATH {config['executable']['python']['script_path']} "
                            f"-params-file {config['executable']['nextflow_param_file']}")
    run_nextflow_command += " -resume" if resume else ""

    run_command_with_output(f"Running submission pipeline: {nextflow_file_to_run}...", run_nextflow_command)
