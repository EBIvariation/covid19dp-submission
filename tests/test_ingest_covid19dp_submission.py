import glob
import os
import unittest

import pymongo
import shutil
import yaml

from covid19dp_submission.ingest_covid19dp_submission import ingest_covid19dp_submission
from covid19dp_submission import ROOT_DIR
from ebi_eva_common_pyutils.command_utils import run_command_with_output
from unittest import TestCase


class TestIngestCovid19DPSubmission(TestCase):

    def __init__(self, *args, **kwargs):
        super(TestIngestCovid19DPSubmission, self).__init__(*args, **kwargs)
        self.resources_folder = os.path.join(ROOT_DIR, 'tests', 'resources')
        self.assembly_report_url = os.path.join(self.resources_folder,
                                                'GCA_009858895.3_ASM985889v3_assembly_report.txt')
        self.fasta_file = os.path.join(self.resources_folder, 'GCA_009858895.3_ASM985889v3_genomic.fna')
        self.nextflow_config_file = os.path.join(self.resources_folder, 'nf.config')

        self.download_folder = os.path.join(self.resources_folder, 'download_snapshot')
        self.download_target_dir = os.path.join(self.download_folder, '30_eva_valid', '2021_07_23_test_snapshot')
        self.download_url = "file:///" + os.path.join(self.resources_folder, 'vcf_files',
                                                      '2021_07_23_test_snapshot.tar.gz')
        self.processing_folder = os.path.join(self.resources_folder, 'processing')
        shutil.rmtree(self.download_folder, ignore_errors=True)
        shutil.rmtree(self.processing_folder, ignore_errors=True)
        os.makedirs(self.processing_folder)

        self.accessioning_database_name = "eva_accession"
        self.accessioning_properties_file = os.path.join(self.processing_folder, 'accessioning.properties')
        self.clustering_properties_file = os.path.join(self.processing_folder, 'clustering.properties')
        self.release_properties_file = os.path.join(self.processing_folder, 'release.properties')
        self.app_config_file = os.path.join(self.processing_folder, 'app_config.yml')
        accessioning_properties = open(os.path.join(self.resources_folder, 'properties', 'accessioning.properties'))\
            .read().format(**self.__dict__)
        clustering_properties = open(os.path.join(self.resources_folder, 'properties', 'clustering.properties'))\
            .read().format(**self.__dict__)
        release_properties = open(os.path.join(self.resources_folder, 'properties', 'release.properties')) \
            .read().format(**self.__dict__)
        open(self.accessioning_properties_file, "w").write(accessioning_properties)
        open(self.clustering_properties_file, "w").write(clustering_properties)
        open(self.release_properties_file, "w").write(release_properties)

    def setUp(self) -> None:
        run_command_with_output("Downloading accessioning JAR file...",
                                f'bash -c '
                                f'"cd {self.processing_folder} '
                                f'&& git clone https://github.com/EBIVariation/eva-accession '
                                f'&& cd eva-accession && mvn -q package -DskipTests '
                                f'&& cp eva-accession-pipeline/target/*.jar '
                                f'{self.processing_folder} '
                                f'&& cp eva-accession-clustering/target/*.jar '
                                f'{self.processing_folder} '
                                f'&& cp eva-accession-release/target/*.jar '
                                f'{self.processing_folder} '
                                f'&& cd {self.processing_folder} && rm -rf eva-accession"')
        self.accession_jar_file = glob.glob(f"{self.processing_folder}/eva-accession-pipeline*.jar")[0]
        self.clustering_jar_file = glob.glob(f"{self.processing_folder}/eva-accession-clustering*.jar")[0]
        self.release_jar_file = glob.glob(f"{self.processing_folder}/eva-accession-release*.jar")[0]

        self.app_config = yaml.safe_load(open(os.path.join(self.resources_folder, 'properties', 'app_config.yml'))
                                         .read().format(**self.__dict__))
        yaml.safe_dump(data=self.app_config, stream=open(self.app_config_file, "w"))

        self.mongo_db = pymongo.MongoClient()
        self.mongo_db.drop_database(self.accessioning_database_name)

    def tearDown(self) -> None:
        shutil.rmtree(self.download_folder, ignore_errors=True)
        shutil.rmtree(self.processing_folder, ignore_errors=True)
        self.mongo_db.drop_database(self.accessioning_database_name)

    @unittest.skip
    def test_ingest_covid19dp_submission(self):
        ingest_covid19dp_submission(download_url=self.download_url, snapshot_name=None,
                                    project_dir=self.processing_folder, app_config_file=self.app_config_file,
                                    nextflow_config_file=self.nextflow_config_file, resume=False)
        num_clustered_variants = self.mongo_db[self.accessioning_database_name]['clusteredVariantEntity'] \
            .count_documents(filter={})
        self.assertEqual(54, num_clustered_variants)
        # check if files are synchronized to the ftp dir
        self.assertEqual(2, len(glob.glob(f"{self.app_config['submission']['public_ftp_dir']}/*")))
        num_incremental_release_records = self.mongo_db[self.accessioning_database_name]['releaseRecordEntity']\
            .count_documents(filter={})
        self.assertEqual(54, num_incremental_release_records)
