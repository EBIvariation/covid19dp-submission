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
import sys

from ebi_eva_common_pyutils.command_utils import run_command_with_output
from ebi_eva_common_pyutils.logger import logging_config

logger = logging_config.get_logger(__name__)
logging_config.add_stdout_handler()


def should_skip_asm_check(vcf_file: str) -> bool:
    return int(run_command_with_output(f"Determine if the VCF file is empty...",
                                       f"zcat {vcf_file} | grep -v ^# | wc -l",
                                       return_process_output=True)) == 0


def run_asm_checker(vcf_file: str, assembly_checker_binary: str, assembly_report: str, assembly_fasta: str,
                    output_dir: str) -> None:
    os.makedirs(name=output_dir, exist_ok=True)
    assembly_check__output_prefix = os.path.basename(vcf_file)
    # This log file captures the status of the overall validation process
    process_log_file_name = f"{output_dir}/{assembly_check__output_prefix}.assembly_check.log"

    if should_skip_asm_check(vcf_file):
        logger.info(f"VCF file {vcf_file} does not have any variants. Skipping assembly check...")
        sys.exit(0)
    run_command_with_output(f"Assembly checking VCF file {vcf_file}...",
                            f'bash -c "{assembly_checker_binary} -i {vcf_file}  '
                            f'-f {assembly_fasta} '
                            f'-a {assembly_report} '
                            f'-r summary,text,valid '
                            f'-o {output_dir} '
                            f'--require-genbank > {process_log_file_name} 2>&1"')


def main():
    parser = argparse.ArgumentParser(description='Assembly check a VCF file',
                                     formatter_class=argparse.RawTextHelpFormatter, add_help=False)
    parser.add_argument("--vcf-file", help="Full path to the VCF file", required=True)
    parser.add_argument("--assembly-checker-binary", help="Full path to the assembly checker binary",
                        default="vcf_assembly_checker", required=False)
    parser.add_argument("--assembly-report", help="Full path to the assembly report", required=True)
    parser.add_argument("--assembly-fasta", help="Full path to the assembly FASTA", required=True)
    parser.add_argument("--output-dir", help="Full path to the assembly check output directory", required=True)

    args = parser.parse_args()
    run_asm_checker(args.vcf_file, args.assembly_checker_binary, args.assembly_report, args.assembly_fasta,
                    args.output_dir)


if __name__ == "__main__":
    main()
