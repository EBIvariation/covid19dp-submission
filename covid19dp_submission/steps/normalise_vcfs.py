# Copyright 2023 EMBL - European Bioinformatics Institute
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


def normalise(input_dir: str, vcf_file: str, output_file: str, bcftools_binary: str, refseq_fasta_file: str) -> str:
    vcf_file_name_no_ext = _get_vcf_filename_without_extension(vcf_file)
    vcf_file_name_no_ext_and_path = os.path.basename(vcf_file_name_no_ext)
    # See here: https://github.com/EBIvariation/eva-submission/blob/bb85922fffb4f29fdce501af036ea79ec8712121/eva_submission/nextflow/prepare_brokering.nf#L141
    commands = [f'cd {input_dir}',
                f'{bcftools_binary} norm --check-ref w --fasta-ref {refseq_fasta_file} --output-type z --output '
                    f'{output_file} {vcf_file} ',
                f'{bcftools_binary} index --force --csi {output_file}'
                ]
    normalise_command = ' && '.join(commands)
    run_command_with_output(f"Normalising {vcf_file_name_no_ext_and_path}...", normalise_command)
    return f"{vcf_file_name_no_ext}.vcf.gz"


def normalise_all(input_dir:str, vcf_files: list, output_dir: str, bcftools_binary: str,
                  refseq_fasta_file: str) -> list:
    output_vcf_files = []
    os.makedirs(name=output_dir, exist_ok=True)
    for vcf_file in vcf_files:
        vcf_file_name_no_ext = _get_vcf_filename_without_extension(vcf_file)
        output_file = f"{output_dir}/{os.path.basename(vcf_file_name_no_ext)}.vcf.gz"
        normalise(input_dir, vcf_file, output_file, bcftools_binary, refseq_fasta_file)
        output_vcf_files.append(output_file)
    return output_vcf_files


def main():
    parser = argparse.ArgumentParser(description='Normalise VCF file',
                                     formatter_class=argparse.RawTextHelpFormatter, add_help=False)
    parser.add_argument("--vcf-files", help="Path to the VCF file (ex: /path/to/file.vcf)",
                        nargs='+', required=True)
    parser.add_argument("--input-dir", help="Path to the directory that will contain the input files",
                        required=True)
    parser.add_argument("--output-dir", help="Path to the directory that will contain the output files",
                        required=True)
    parser.add_argument("--bcftools-binary", help="Full path to the bcftools binary (ex: /path/to/bcftools)",
                        default="bcftools", required=False)
    parser.add_argument("--refseq-fasta-file", help="Path to the RefSeq FASTA file (ex: /path/to/refseq_fasta.fa)",
                        required=True)
    args = parser.parse_args()
    logging_config.add_stdout_handler()

    normalise_all(args.input_dir, args.vcf_files, args.output_dir, args.bcftools_binary, args.refseq_fasta_file)


if __name__ == "__main__":
    main()
