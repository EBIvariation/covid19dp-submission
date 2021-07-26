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

from ebi_eva_common_pyutils.command_utils import run_command_with_output
from ebi_eva_common_pyutils.logger import logging_config

logger = logging_config.get_logger(__name__)
logging_config.add_stdout_handler()


def cluster_assembly(clustering_assembly: str, clustering_jar_file: str, clustering_properties_file: str,
                     memory: int) -> None:
    clustering_command = f"java -Xmx{memory}g -jar {clustering_jar_file} " \
                         f"--spring.config.location={clustering_properties_file} " \
                         f"--parameters.assemblyAccession={clustering_assembly}"
    run_command_with_output(f"Running clustering with properties file: {clustering_properties_file}...",
                            clustering_command)


def main():
    parser = argparse.ArgumentParser(description='Cluster an assembly',
                                     formatter_class=argparse.RawTextHelpFormatter, add_help=False)
    parser.add_argument("--clustering-assembly", help="Clustering assembly (ex: GCA_009858895.3)", required=True)
    parser.add_argument("--clustering-jar-file",
                        help="Full path to the accessioning JAR file (ex: /path/to/eva-accession-pipeline.jar)",
                        required=True)
    parser.add_argument("--clutering-properties-file",
                        help="Full path to the accessioning configuration (ex: /path/to/accessioning.properties)",
                        required=True)
    parser.add_argument("--memory", help="Memory allocation (in GB)", type=int, default=16, required=False)
    args = parser.parse_args()
    cluster_assembly(args.clustering_assembly, args.clustering_jar_file, args.clustering_properties_file, args.memory)


if __name__ == "__main__":
    main()
