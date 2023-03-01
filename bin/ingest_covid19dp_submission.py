#!/usr/bin/env python
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

from ebi_eva_common_pyutils.logger import logging_config

from covid19dp_submission.ingest_covid19dp_submission import ingest_covid19dp_submission


def main():
    parser = argparse.ArgumentParser(description='Ingest a snapshot submission from the Covid-19 data portal project')
    parser.add_argument("--project", required=True,
                        help="project from which analyses needs to be downloaded")
    parser.add_argument("--project-dir", help="Project directory (ex: /path/to/PRJ)", default=None, required=True)
    parser.add_argument("--num-analyses", type=int, default=10000, required=False,
                        help="Number of analyses to download (max = 10000)")
    parser.add_argument("--processed-analyses-file", required=True,
                        help="full path to the file containing all the processed analyses")
    parser.add_argument("--ignored-analyses-file", required=True,
                        help="full path to the file containing a list of analyses to skip when processing.")
    parser.add_argument("--accepted-taxonomies", required=True, nargs='+', type=int,
                        help="taxonomy id of the data that should be downloaded from ENA")
    parser.add_argument("--app-config-file",
                        help="Full path to the application config file (ex: /path/to/config.yml)", required=True)
    parser.add_argument("--nextflow-config-file",
                        help="Full path to the Nextflow config file", default=None, required=False)
    parser.add_argument("--resume-snapshot", type=str, required=False,
                        help="Resume a previous job. You need to specify the snapshot name to be resumed "
                             "(ex: 2021_06_28_14_28_56)")
    args = parser.parse_args()
    logging_config.add_stdout_handler()

    ingest_covid19dp_submission(args.project, args.project_dir, args.num_analyses,
                                args.processed_analyses_file, args.ignored_analyses_file, args.accepted_taxonomies,
                                args.app_config_file, args.nextflow_config_file,
                                args.resume_snapshot)


if __name__ == "__main__":
    main()
