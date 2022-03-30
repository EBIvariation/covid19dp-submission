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

from covid19dp_submission.ingest_covid19dp_submission import ingest_covid19dp_submission


def main():
    parser = argparse.ArgumentParser(description='Ingest a snapshot submission from the Covid-19 data portal project',
                                     formatter_class=argparse.RawTextHelpFormatter, add_help=False)
    parser.add_argument("--project", default='PRJEB45554', required=False,
                        help="project from which analyses needs to be downloaded")
    parser.add_argument("--snapshot-name", help="Snapshot name (ex: 2021_06_28_14_28_56)", default=None,
                        required=False)
    parser.add_argument("--project-dir", help="Project directory (ex: /path/to/PRJ)", default=None, required=True)
    parser.add_argument("--num-analyses", type=int, default=10000, required=False,
                        help="Number of analyses to download (max = 10000)")
    parser.add_argument("--processed-analyses-file", required=True,
                        help="full path to the file containing all the processed analyses")
    parser.add_argument("--app-config-file",
                        help="Full path to the application config file (ex: /path/to/config.yml)", required=True)
    parser.add_argument("--nextflow-config-file",
                        help="Full path to the Nextflow config file", default=None, required=False)
    parser.add_argument("--resume",
                        help="Indicate if a previous concatenation job is to be resumed", action='store_true',
                        required=False)
    args = parser.parse_args()
    ingest_covid19dp_submission(args.project, args.snapshot_name, args.project_dir, args.num_analyses,
                                args.processed_analyses_file, args.app_config_file, args.nextflow_config_file,
                                args.resume)


if __name__ == "__main__":
    main()
