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
import glob
import os
import urllib

import requests
from ebi_eva_common_pyutils.logger import logging_config
from retry import retry

logger = logging_config.get_logger(__name__)


def download_analyses(project, num_analyses, processed_analyses_file, download_target_dir):
    total_analyses = total_analyses_in_project(project)
    logger.info(f"total analyses in project {project}: {total_analyses}")

    analyses_array = get_analyses_to_process(project, num_analyses, total_analyses, processed_analyses_file)
    logger.info(f"number of analyses to process: {len(analyses_array)}")

    os.makedirs(download_target_dir, exist_ok=True)
    download_files(analyses_array, download_target_dir, processed_analyses_file)

    vcf_files_downloaded = glob.glob(f"{download_target_dir}/*.vcf")
    logger.info(f"total number of files downloaded: {len(vcf_files_downloaded)}")

    if len(analyses_array) != len(vcf_files_downloaded):
        raise logger.warn(f"Not all analyses were downloaded. Num of Analyses to download={len(analyses_array)},"
                          f"Num of Actual Analyses downloaded = {len(vcf_files_downloaded)}, "
                          f"Analyses to download = {analyses_array}"
                          f"Downloaded Analyses = {[os.path.basename(x) for x in vcf_files_downloaded]}")

    return download_target_dir


@retry(logger=logger, tries=4, delay=120, backoff=1.2, jitter=(1, 3))
def total_analyses_in_project(project):
    count_url = f"https://www.ebi.ac.uk/ena/portal/api/filereportcount?accession={project}&result=analysis"
    response = requests.get(count_url)
    if response.status_code != 200:
        logger.error(f"Error fetching total analyses count for project {project}")
        response.raise_for_status()
    return response.json()


def get_analyses_to_process(project, num_analyses, total_analyses, processed_analyses_file):
    offset = 0
    limit = 100000
    processed_analyses = get_processed_analyses(processed_analyses_file)
    analyses_for_processing = []
    while offset < total_analyses:
        logger.info(f"Fetching ENA analyses from {offset} to  {offset + limit} (offset={offset}, limit={limit})")
        analyses_from_ena = get_analyses_from_ena(project, offset, limit)
        unprocessed_analyses = filter_out_processed_analyses(analyses_from_ena, processed_analyses)
        logger.info(
            f"number of analyses already processed in current iteration: {len(analyses_from_ena) - len(unprocessed_analyses)}")

        if (len(analyses_for_processing) + len(unprocessed_analyses)) >= num_analyses:
            analyses_for_processing = analyses_for_processing + \
                                      unprocessed_analyses[:(num_analyses - len(analyses_for_processing))]
            logger.info(f"Number of analyses found for processing till now : {len(analyses_for_processing)}")
            break
        else:
            analyses_for_processing = analyses_for_processing + unprocessed_analyses
            logger.info(f"Number of analyses found for processing till now : {len(analyses_for_processing)}")
            offset = offset + limit

    return analyses_for_processing


@retry(logger=logger, tries=4, delay=120, backoff=1.2, jitter=(1, 3))
def get_analyses_from_ena(project, offset, limit):
    analyses_url = f"https://www.ebi.ac.uk/ena/portal/api/filereport?result=analysis&accession={project}&offset={offset}" \
                   f"&limit={limit}&format=json&fields=run_ref,analysis_accession,submitted_ftp"
    response = requests.get(analyses_url)
    if response.status_code != 200:
        logger.error(f"Error fetching analyses info from ENA for {project}")
        response.raise_for_status()
    return response.json()


def filter_out_processed_analyses(analyses_array, processed_analyses):
    unprocessed_analyses = []
    for analysis in analyses_array:
        if analysis['analysis_accession'] not in processed_analyses:
            unprocessed_analyses.append(analysis)

    return unprocessed_analyses


def get_processed_analyses(processed_analyses_file):
    processed_analyses = set()
    with open(processed_analyses_file, 'r') as file:
        for line in file:
            processed_analyses.add(line.split(",")[0])
    return processed_analyses


def download_files(analyses_array, download_target_dir, processed_analyses_file):
    logger.info(f"total number of files to download: {len(analyses_array)}")
    with open(processed_analyses_file, 'a') as f:
        for analysis in analyses_array:
            download_url = f"http://{analysis['submitted_ftp']}"
            download_file_name = f"{analysis['analysis_accession']}.vcf"
            download_file_path = f"{download_target_dir}/{download_file_name}"
            try:
                logger.info(f"downloading file {download_url}")
                download_file(download_url, download_file_path)
                logger.info(f"downloaded file {download_file_name}")
                f.write(f"\n{analysis['analysis_accession']},{analysis['submitted_ftp']}")
            except:
                logger.warning(f"Could not download file : {download_file_path}")
                if os.path.exists(download_file_path):
                    os.remove(download_file_path)


@retry(logger=logger, tries=4, delay=120, backoff=1.2, jitter=(1, 3))
def download_file(download_url, download_file_path):
    urllib.request.urlretrieve(download_url, download_file_path)


def main():
    parser = argparse.ArgumentParser(description='Download analyses for processing from the Covid-19 DP project',
                                     formatter_class=argparse.RawTextHelpFormatter, add_help=False)
    parser.add_argument("--project", default='PRJEB45554', required=False,
                        help="project from which analyses needs to be downloaded")
    parser.add_argument("--num-analyses", type=int, default=10000, required=False,
                        help="Number of analyses to download (max = 10000)")
    parser.add_argument("--processed-analyses-file", required=True,
                        help="full path to the file containing all the processed analyses")
    parser.add_argument("--download-target-dir", required=True, help="Full path to the target download directory")

    args = parser.parse_args()
    logging_config.add_stdout_handler()

    if args.num_analyses < 1 or args.num_analyses > 10000:
        raise Exception("number of analyses to download should be between 1 and 10000")

    download_analyses(args.project, args.num_analyses, args.processed_analyses_file, args.download_target_dir)


if __name__ == "__main__":
    main()
