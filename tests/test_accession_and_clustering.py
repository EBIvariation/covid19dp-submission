import glob
import os
import pymongo
import shutil

from covid19dp_submission.steps.accession_vcf import accession_vcf
from covid19dp_submission.steps.cluster_assembly import cluster_assembly
from covid19dp_submission.steps.download_snapshot import download_snapshot
from covid19dp_submission import ROOT_DIR
from ebi_eva_common_pyutils.command_utils import run_command_with_output
from unittest import TestCase


class TestAccessionVcf(TestCase):

    def __init__(self, *args, **kwargs):
        super(TestAccessionVcf, self).__init__(*args, **kwargs)
        self.resources_folder = os.path.join(ROOT_DIR, 'tests', 'resources')
        self.assembly_report_url = os.path.join(self.resources_folder,
                                                'GCA_009858895.3_ASM985889v3_assembly_report.txt')
        self.fasta_file = os.path.join(self.resources_folder, 'GCA_009858895.3_ASM985889v3_genomic.fna')

        self.download_folder = os.path.join(self.resources_folder, 'download_snapshot')
        self.download_target_dir = os.path.join(self.download_folder, '30_eva_valid', '2021_07_23_test_snapshot')
        self.download_url = "file:///" + os.path.join(self.resources_folder, 'vcf_files',
                                                      '2021_07_23_test_snapshot.tar.gz')
        self.processing_folder = os.path.join(self.resources_folder, 'processing')
        shutil.rmtree(self.download_folder, ignore_errors=True)
        shutil.rmtree(self.processing_folder, ignore_errors=True)
        os.makedirs(self.processing_folder)

        self.accessioning_database_name = "eva_accession"
        self.accessioning_properties_file = os.path.join(self.processing_folder, 'accession_config.properties')
        self.clustering_properties_file = os.path.join(self.processing_folder, 'clustering.properties')
        self.accessioning_properties = open(os.path.join(self.resources_folder, 'properties',
                                                         'accessioning.properties')).read().format(**self.__dict__)
        self.clustering_properties = open(os.path.join(self.resources_folder, 'properties', 'clustering.properties'))\
            .read().format(**self.__dict__)
        open(self.accessioning_properties_file, "w").write(self.accessioning_properties)
        open(self.clustering_properties_file, "w").write(self.clustering_properties)

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
                                f'&& cd {self.processing_folder} && rm -rf eva-accession"')
        self.accession_jar_file = glob.glob(f"{self.processing_folder}/eva-accession-pipeline*.jar")[0]
        self.clustering_jar_file = glob.glob(f"{self.processing_folder}/eva-accession-clustering*.jar")[0]

        self.mongo_db = pymongo.MongoClient()
        self.mongo_db.drop_database(self.accessioning_database_name)

    def tearDown(self) -> None:
        shutil.rmtree(self.download_folder, ignore_errors=True)
        shutil.rmtree(self.processing_folder, ignore_errors=True)
        self.mongo_db.drop_database(self.accessioning_database_name)

    def test_accession_and_clustering(self):
        download_dir = download_snapshot(download_url=self.download_url, snapshot_name=None,
                                         download_target_dir=self.download_target_dir)
        vcf_file = f"{download_dir}/file1_test_snapshot.vcf.gz"
        output_vcf_file = f"{self.processing_folder}/output.accessioned.vcf"
        accession_vcf(input_vcf_file=vcf_file, accessioning_jar_file=self.accession_jar_file,
                      accessioning_properties_file=self.accessioning_properties_file,
                      accessioning_instance="instance-10", output_vcf_file=output_vcf_file, bcftools_binary="bcftools",
                      memory=8)
        cluster_assembly(clustering_jar_file=self.clustering_jar_file,
                         clustering_properties_file=self.clustering_properties_file,
                         accessioning_instance="instance-10", memory=8)
        num_clustered_variants = self.mongo_db[self.accessioning_database_name]['clusteredVariantEntity']\
            .count_documents(filter={})
        self.assertEqual(15, num_clustered_variants)
