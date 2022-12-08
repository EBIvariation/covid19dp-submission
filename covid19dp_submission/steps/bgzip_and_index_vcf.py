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

logger = logging_config.get_logger(__name__)


def _get_vcf_filename_without_extension(vcf_file_name: str) -> str:
    return vcf_file_name.replace(".vcf.gz", "").replace(".vcf", "")


def bgzip_and_index(vcf_files: list, output_dir: str, bcftools_binary: str) -> list:
    output_vcf_files = []
    os.makedirs(name=output_dir, exist_ok=True)
    for vcf_file in vcf_files:
        vcf_file_name_no_ext = _get_vcf_filename_without_extension(vcf_file)
        output_file = f"{output_dir}/{vcf_file_name_no_ext}.vcf.gz"
        vcf_file_name_no_ext_and_path = os.path.basename(vcf_file_name_no_ext)
        # Uncompress VCF file if it is already compressed (usually gzip) or proceed
        bgzip_and_index_command = f'bash -c "(gunzip {vcf_file} || true) && ' \
                                  f'{bcftools_binary} convert {vcf_file_name_no_ext}.vcf -O z ' \
                                  f'-o {output_file} && ' \
                                  f'{bcftools_binary} index --csi {output_file} && ' \
                                  f'rm -rf {vcf_file_name_no_ext}.vcf"'
        run_command_with_output(f"BGZipping and indexing {vcf_file_name_no_ext_and_path}...", bgzip_and_index_command)
        output_vcf_files.append(f"{vcf_file_name_no_ext}.vcf.gz")
    return output_vcf_files


def main():
    parser = argparse.ArgumentParser(description='BGZip and index VCF file',
                                     formatter_class=argparse.RawTextHelpFormatter, add_help=False)
    parser.add_argument("--vcf-file", help="Path to the VCF file (ex: /path/to/file.vcf)",
                        required=True)
    parser.add_argument("--output-dir", help="Path to the directory that will contains the output files",
                        required=True)
    parser.add_argument("--bcftools-binary", help="Full path to the bcftools binary (ex: /path/to/bcftools)",
                        default="bcftools", required=False)
    args = parser.parse_args()
    logging_config.add_stdout_handler()

    bgzip_and_index(args.vcf_file, args.output_dir, args.bcftools_binary)


if __name__ == "__main__":
    main()
