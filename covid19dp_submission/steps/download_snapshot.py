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
import glob
import os

from ebi_eva_common_pyutils.command_utils import run_command_with_output
from ebi_eva_common_pyutils.logger import logging_config
from retry import retry
from subprocess import CalledProcessError

logger = logging_config.get_logger(__name__)


@retry(exceptions=CalledProcessError, logger=logger, tries=4, delay=240, backoff=1.2, jitter=(1, 3))
def download_snapshot(download_url: str, snapshot_name: str or None, download_target_dir: str) -> str:
    snapshot_name = snapshot_name if snapshot_name else os.path.basename(download_url).replace(".tar.gz", "")
    assert snapshot_name, "Snapshot name cannot be empty!"

    os.makedirs(download_target_dir, exist_ok=True)
    if glob.glob(f"{download_target_dir}/*.vcf.gz"):
        raise FileExistsError(f"FAIL: Snapshot already downloaded to target directory: {download_target_dir}. "
                              f"Please delete that directory to re-download.")
    download_file_name = os.path.basename(download_url)

    # Use strip-components switch to avoid extracting with the directory structure
    # since we have already created the requisite directory and passed it to download_target_dir
    snapshot_download_command = f'bash -c "cd {download_target_dir} && curl -O {download_url} && ' \
                                f'tar xzf {download_file_name} --strip-components=1 && rm -rf {download_file_name}"'
    run_command_with_output(f"Downloading data snapshot {snapshot_name}...", snapshot_download_command)
    return download_target_dir


def main():
    parser = argparse.ArgumentParser(description='Download a data snapshot from the Covid-19 DP project',
                                     formatter_class=argparse.RawTextHelpFormatter, add_help=False)
    parser.add_argument("--download-url",
                        help="URL to the data snapshot (ex: http://path/to/snapshots/YYYY_MM_DD.tar.gz)", required=True)
    parser.add_argument("--snapshot-name", help="Snapshot name (ex: 2021_06_28_filtered_vcf)", default=None,
                        required=False)
    parser.add_argument("--download-target-dir", help="Full path to the target download directory", required=True)

    args = parser.parse_args()
    logging_config.add_stdout_handler()
    download_snapshot(args.download_url, args.snapshot_name, args.download_target_dir)


if __name__ == "__main__":
    main()
