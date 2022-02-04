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

from .bgzip_and_index_vcf import bgzip_and_index
from ebi_eva_common_pyutils.command_utils import run_command_with_output
from ebi_eva_common_pyutils.logger import logging_config

logger = logging_config.get_logger(__name__)


def is_input_same_as_output_vcfs(input_vcf_file: str, output_vcf_file: str) -> bool:
    """
    Ensure that the accessioned VCF file and the input VCF files have the same number of records
    """
    num_entries_in_input = int(run_command_with_output(f"Count entries from input file: {input_vcf_file}...",
                                                       f"zcat {input_vcf_file} | grep -v ^# | wc -l",
                                                       return_process_output=True))
    num_entries_in_output = int(run_command_with_output(f"Count entries from output file: {output_vcf_file}...",
                                                        f"zcat {output_vcf_file} | grep -v ^# | wc -l",
                                                        return_process_output=True))
    return num_entries_in_output == num_entries_in_input


def accession_vcf(input_vcf_file: str, accessioning_jar_file: str, accessioning_properties_file: str,
                  accessioning_instance: str, output_vcf_file: str, bcftools_binary: str, memory: int) -> str:
    assert not os.path.exists(output_vcf_file), f"FAIL: Output VCF file {output_vcf_file} already exists."
    accession_command = f"java -Xmx{memory}g -jar {accessioning_jar_file} " \
                        f"--spring.config.location={accessioning_properties_file} " \
                        f"--parameters.vcf={input_vcf_file} " \
                        f"--parameters.outputVcf={output_vcf_file} " \
                        f"--accessioning.instanceId={accessioning_instance}"
    run_command_with_output(f"Running accession for file: {input_vcf_file}...", accession_command)
    compressed_output_vcf_file = bgzip_and_index(output_vcf_file, bcftools_binary)
    if not is_input_same_as_output_vcfs(input_vcf_file, compressed_output_vcf_file):
        logger.warning(f"Number of accessioned variants in {compressed_output_vcf_file} "
                       f"does not equal number of variants in the input file {input_vcf_file}.")
    logger.info(f"Accessioned file is in: {compressed_output_vcf_file}")
    return compressed_output_vcf_file


def main():
    parser = argparse.ArgumentParser(description='Accession a VCF file',
                                     formatter_class=argparse.RawTextHelpFormatter, add_help=False)
    parser.add_argument("--vcf-file", help="Full path to the VCF file (ex: /path/to/file.vcf)", required=True)
    parser.add_argument("--accessioning-jar-file",
                        help="Full path to the accessioning JAR file (ex: /path/to/eva-accession-pipeline.jar)",
                        required=True)
    parser.add_argument("--accessioning-properties-file",
                        help="Full path to the accessioning configuration (ex: /path/to/accessioning.properties)",
                        required=True)
    parser.add_argument("--accessioning-instance",help="Instance to be used for accession (ex: instance-10)",
                        required=True)
    parser.add_argument("--output-vcf-file",
                        help="Full path to the output VCF file (ex: /path/to/accessioned_output_file.vcf)",
                        required=True)
    parser.add_argument("--bcftools-binary", help="Full path to the bcftools binary (ex: /path/to/bcftools)",
                        default="bcftools", required=False)
    parser.add_argument("--memory", help="Memory allocation (in GB)", type=int, default=16, required=False)
    args = parser.parse_args()
    logging_config.add_stdout_handler()
    accession_vcf(args.vcf_file, args.accessioning_jar_file, args.accessioning_properties_file,
                  args.accessioning_instance, args.output_vcf_file, args.bcftools_binary, args.memory)


if __name__ == "__main__":
    main()
