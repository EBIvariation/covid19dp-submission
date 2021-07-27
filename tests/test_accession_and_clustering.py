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
    resources_folder = os.path.join(ROOT_DIR, 'tests', 'resources')
    assembly_report_url = os.path.join(resources_folder, 'GCA_009858895.3_ASM985889v3_assembly_report.txt')
    fasta_file = os.path.join(resources_folder, 'GCA_009858895.3_ASM985889v3_genomic.fna')

    download_folder = os.path.join(resources_folder, 'download_snapshot')
    download_url = "file:///" + os.path.join(resources_folder, 'vcf_files', '2021_07_23_test_snapshot.tar.gz')

    processing_folder = os.path.join(resources_folder, 'processing')
    accessioning_database_name = "eva_accession"
    accessioning_properties_file = os.path.join(processing_folder, 'accession_config.properties')
    clustering_properties_file = os.path.join(processing_folder, 'clustering.properties')
    accessioning_properties = f"""parameters.projectAccession=PRJEB43947
        accessioning.instanceId=instance-1
        spring.main.allow-bean-definition-overriding=true
        spring.datasource.url=jdbc:postgresql://localhost:5432/postgres
        spring.jpa.generate-ddl=true
        parameters.assemblyAccession=GCA_009858895.3
        spring.data.mongodb.database={accessioning_database_name}
        parameters.contigNaming=NO_REPLACEMENT
        parameters.taxonomyAccession=2697049        
        parameters.forceRestart=false
        parameters.assemblyReportUrl=file:{assembly_report_url}
        accessioning.monotonic.ss.blockSize=100000
        spring.datasource.password=postgres
        spring.datasource.driver-class-name=org.postgresql.Driver
        accessioning.monotonic.ss.nextBlockInterval=1000000000
        spring.main.web-environment=false
        accessioning.monotonic.ss.blockStartValue=5000000000
        spring.datasource.username=postgres
        spring.data.mongodb.port=27017
        mongodb.read-preference=primary
        parameters.chunkSize=100        
        spring.datasource.tomcat.max-active=3
        spring.data.mongodb.host=localhost
        parameters.fasta={fasta_file}
        parameters.vcfAggregation=BASIC
        spring.batch.job.names=CREATE_SUBSNP_ACCESSION_JOB
        spring.jpa.properties.hibernate.jdbc.lob.non_contextual_creation=true
        accessioning.submitted.categoryId=ss
        spring.main.web-application-type=NONE
        spring.batch.initialize-schema=always
        """
    clustering_properties = """spring.batch.job.names=CLUSTERING_FROM_MONGO_JOB
        parameters.vcf=
        parameters.projectAccession=
        parameters.chunkSize=100
        
        accessioning.instanceId=instance-1
        accessioning.submitted.categoryId=ss
        accessioning.clustered.categoryId=rs
        
        accessioning.monotonic.ss.blockSize=100000
        accessioning.monotonic.ss.blockStartValue=5000000000
        accessioning.monotonic.ss.nextBlockInterval=1000000000
        accessioning.monotonic.rs.blockSize=100000
        accessioning.monotonic.rs.blockStartValue=3000000000
        accessioning.monotonic.rs.nextBlockInterval=1000000000
            
        spring.data.mongodb.host=localhost
        spring.data.mongodb.port=27017
        spring.data.mongodb.database=eva_accession
        mongodb.read-preference=primary
            
        spring.datasource.driver-class-name=org.postgresql.Driver
        spring.datasource.url=jdbc:postgresql://localhost:5432/postgres
        spring.datasource.username=postgres
        spring.datasource.password=postgres
        spring.datasource.tomcat.max-active=3
            
        #See https://github.com/spring-projects/spring-boot/wiki/Spring-Boot-2.1-Release-Notes#bean-overriding
        spring.main.allow-bean-definition-overriding=true
        #As this is a spring batch application, disable the embedded tomcat. This is the new way to do that for spring 2.
        spring.main.web-application-type=none
        spring.main.web-environment=false
        
        spring.jpa.generate-ddl=true
        # This entry is put just to avoid a warning message in the logs when you start the spring-boot application.
        # This bug is from hibernate which tries to retrieve some metadata from postgresql db and failed to find that and logs as a warning
        # It doesnt cause any issue though.
        spring.jpa.properties.hibernate.jdbc.lob.non_contextual_creation = true
        """

    def setUp(self) -> None:
        shutil.rmtree(self.download_folder, ignore_errors=True)
        shutil.rmtree(self.processing_folder, ignore_errors=True)
        os.makedirs(self.processing_folder)

        run_command_with_output("Downloading accessioning JAR file...",
                                f'bash -c '
                                f'"cd {self.processing_folder} && git clone https://github.com/EBIVariation/eva-accession '
                                f'&& cd eva-accession && mvn -q package -DskipTests '
                                f'&& cp eva-accession-pipeline/target/*.jar '
                                f'{self.processing_folder} '
                                f'&& cp eva-accession-clustering/target/*.jar '
                                f'{self.processing_folder} '
                                f'&& cd {self.processing_folder} && rm -rf eva-accession"')
        self.accession_jar_file = glob.glob(f"{self.processing_folder}/eva-accession-pipeline*.jar")[0]
        self.clustering_jar_file = glob.glob(f"{self.processing_folder}/eva-accession-clustering*.jar")[0]
        open(self.accessioning_properties_file, "w").write(self.accessioning_properties)
        open(self.clustering_properties_file, "w").write(self.clustering_properties)

        self.mongo_db = pymongo.MongoClient()

    def tearDown(self) -> None:
        shutil.rmtree(self.download_folder, ignore_errors=True)
        shutil.rmtree(self.processing_folder, ignore_errors=True)
        self.mongo_db.drop_database(self.accessioning_database_name)

    def test_accession_and_clustering(self):
        download_dir = download_snapshot(download_url=self.download_url, snapshot_name=None,
                                         project_dir=self.download_folder)
        vcf_files = glob.glob(f"{download_dir}/*.vcf.gz")
        output_vcf_file = f"{self.processing_folder}/output.accessioned.vcf"
        accession_vcf(input_vcf_file=vcf_files[0], accessioning_jar_file=self.accession_jar_file,
                      accessioning_properties_file=self.accessioning_properties_file,
                      output_vcf_file=output_vcf_file, bcftools_binary="bcftools", memory=8)
        cluster_assembly(clustering_assembly="GCA_009858895.3", clustering_jar_file=self.clustering_jar_file,
                         clustering_properties_file=self.clustering_properties_file, memory=8)
        num_clustered_variants = self.mongo_db[self.accessioning_database_name]['clusteredVariantEntity']\
            .count_documents(filter={})
        self.assertEqual(15, num_clustered_variants)
