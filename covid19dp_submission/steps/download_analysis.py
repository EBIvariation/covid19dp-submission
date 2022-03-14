# Copyright 2022 EMBL - European Bioinformatics Institute
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
import urllib

import requests
from ebi_eva_common_pyutils.logger import logging_config
from retry import retry

logger = logging_config.get_logger(__name__)


def download_analysis(project, num_analysis, processed_analysis_file, download_target_dir):
    total_analysis = total_analysis_in_project(project)
    logger.info(f"total analysis in project {project}: {total_analysis}")

    analysis_array = get_analysis_to_process(project, num_analysis, total_analysis, processed_analysis_file)
    logger.info(f"number of analysis to process: {len(analysis_array)}")

    download_files(analysis_array, download_target_dir, processed_analysis_file)

@retry(logger=logger, tries=4, delay=120, backoff=1.2, jitter=(1, 3))
def total_analysis_in_project(project):
    count_url = f"https://www.ebi.ac.uk/ena/portal/api/filereportcount?accession={project}&result=analysis"
    response = requests.get(count_url)
    if response.status_code != 200:
        logger.error(f"Error fetching total analysis count for project {project}")
        response.raise_for_status()
    return response.json()


def get_analysis_to_process(project, num_analysis, total_analysis, processed_analysis_file):
    offset = 0
    limit = num_analysis
    processed_analysis = get_processed_analysis(processed_analysis_file)
    analysis_for_processing = []
    while offset < total_analysis:
        analysis_from_ena = get_analysis_from_ena(project, offset, limit)
        unprocessed_analysis = filter_out_processed_analysis(analysis_from_ena, processed_analysis)

        if len(analysis_for_processing) + len(unprocessed_analysis) >= num_analysis:
            analysis_for_processing = analysis_for_processing + unprocessed_analysis[
                                                                :(num_analysis - len(analysis_for_processing))]
            break
        else:
            analysis_for_processing = analysis_for_processing + unprocessed_analysis
            offset = offset + limit

    return analysis_for_processing

@retry(logger=logger, tries=4, delay=120, backoff=1.2, jitter=(1, 3))
def get_analysis_from_ena(project, offset, limit):
    analysis_url = f"https://www.ebi.ac.uk/ena/portal/api/filereport?result=analysis&accession={project}&offset={offset}" \
                   f"&limit={limit}&format=json&fields=run_ref,analysis_accession,submitted_ftp"
    response = requests.get(analysis_url)
    if response.status_code != 200:
        logger.error(f"Error fetching analysis info from ENA for {project}")
        response.raise_for_status()
    return response.json()


def filter_out_processed_analysis(analysis_array, processed_analysis):
    unprocessed_analysis = []
    for analysis in analysis_array:
        if analysis['analysis_accession'] not in processed_analysis:
            unprocessed_analysis.append(analysis)

    return unprocessed_analysis


def get_processed_analysis(processed_analysis_file):
    processed_analysis = set()
    with open(processed_analysis_file, 'r') as file:
        for line in file:
            processed_analysis.add(line.split(",")[0])
    return processed_analysis


def download_files(analysis_array, download_target_dir, processed_analysis_file):
    logger.info(f"total number of files to download: {len(analysis_array)}")
    with open(processed_analysis_file, 'a') as f:
        for analysis in analysis_array:
            download_url = f"http://{analysis['submitted_ftp']}"
            download_file_name = f"{analysis['analysis_accession']}.vcf"
            download_file_path = f"{download_target_dir}/{download_file_name}"

            logger.info(f"downloading file {download_url}")
            download_file(download_url, download_file_path)

            logger.info(f"downloaded file {download_file_name}")
            f.write(f"{analysis['analysis_accession']},{analysis['submitted_ftp']}\n")


@retry(logger=logger, tries=4, delay=120, backoff=1.2, jitter=(1, 3))
def download_file(download_url, download_file_path):
    urllib.request.urlretrieve(download_url, download_file_path)


def main():
    parser = argparse.ArgumentParser(description='Download analysis for processing from the Covid-19 DP project',
                                     formatter_class=argparse.RawTextHelpFormatter, add_help=False)
    parser.add_argument("--project", default='PRJEB45554', required=False,
                        help="project from which analysis needs to be downloaded")
    parser.add_argument("--num-analysis", type=int, default=10000, required=False,
                        help="Number of analysis to download (max = 10000)")
    parser.add_argument("--processed-analysis-file", required=True,
                        help="full path to the file containing all the processed analysis")
    parser.add_argument("--download-target-dir", required=True, help="Full path to the target download directory")

    args = parser.parse_args()
    logging_config.add_stdout_handler()

    if args.num_analysis < 1 or args.num_analysis > 10000:
        raise Exception("number of analysis to download should be between 1 and 10000")

    download_analysis(args.project, args.num_analysis, args.processed_analysis_file, args.download_target_dir)


if __name__ == "__main__":
    main()
