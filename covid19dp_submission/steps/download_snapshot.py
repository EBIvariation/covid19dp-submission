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
import os

from ebi_eva_common_pyutils.command_utils import run_command_with_output
from ebi_eva_common_pyutils.logger import logging_config
from subprocess import CalledProcessError

logger = logging_config.get_logger(__name__)
logging_config.add_stdout_handler()


def download_snapshot(download_url, snapshot_name, project_dir):
    snapshot_name = snapshot_name if snapshot_name else os.path.basename(download_url).replace(".tar.gz", "")
    assert snapshot_name, "Snapshot name cannot be empty!"

    download_target_dir = os.path.join(project_dir, '30_eva_valid', snapshot_name)
    download_file_name = os.path.basename(download_url)

    # Use strip-components switch to avoid extracting with the directory structure
    # since we have already created the requisite directory and passed it to download_target_dir
    snapshot_download_command = f'bash -c "cd {download_target_dir} && curl -O {download_file_name} && ' \
                                f'tar xzvf {download_file_name} --strip-components=1"'
    run_command_with_output(f"Downloading data snapshot {snapshot_name}...", snapshot_download_command)


def main():
    parser = argparse.ArgumentParser(description='Download a data snapshot from the Covid-19 DP project',
                                     formatter_class=argparse.RawTextHelpFormatter, add_help=False)
    parser.add_argument("--download-url",
                        help="URL to the data snapshot (ex: http://path/to/snapshots/YYYY_MM_DD.tar.gz)", required=True)
    parser.add_argument("--snapshot-name", help="Snapshot name (ex: 2021_06_28_filtered_vcf)", default=None,
                        required=False)
    parser.add_argument("--project-dir", help="Full path to the PRJEB directory", required=True)

    args = parser.parse_args()
    download_snapshot(args.download_url, args.snapshot_name, args.project_dir)


if __name__ == "__main__":
    main()
