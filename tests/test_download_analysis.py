import glob
import os
import shutil
from unittest import TestCase

from covid19dp_submission import ROOT_DIR
from covid19dp_submission.steps.download_analyses import download_analyses


class TestDownloadSnapshot(TestCase):
    resources_folder = os.path.join(ROOT_DIR, 'tests', 'resources')
    toplevel_download_folder = os.path.join(resources_folder, 'download_analyses')
    download_target_dir = os.path.join(toplevel_download_folder, '30_eva_valid')
    processed_analyses_file = os.path.join(toplevel_download_folder, 'processed_analyses_file.txt')
    project = 'PRJEB45554'
    num_analyses_to_download = 5

    def setUp(self) -> None:
        shutil.rmtree(self.toplevel_download_folder, ignore_errors=True)
        os.makedirs(self.download_target_dir)

    def tearDown(self) -> None:
        shutil.rmtree(self.toplevel_download_folder, ignore_errors=True)

    def get_processed_files_data(self):
        return ["ERZ3372540,ftp.sra.ebi.ac.uk/vol1/ERZ337/ERZ3372540/SRR15239121.vcf",
                "\nERZ3372549,ftp.sra.ebi.ac.uk/vol1/ERZ337/ERZ3372549/ERR6259542.vcf",
                "\nERZ3372550,ftp.sra.ebi.ac.uk/vol1/ERZ337/ERZ3372550/ERR6259546.vcf",
                "\nERZ3372551,ftp.sra.ebi.ac.uk/vol1/ERZ337/ERZ3372551/ERR6259557.vcf",
                "\nERZ3400189,ftp.sra.ebi.ac.uk/vol1/ERZ340/ERZ3400189/SRR15239121.vcf"
                ]

    def create_processed_analysis_file(self, data):
        with open(self.processed_analyses_file, 'w+') as f:
            for entry in data:
                f.write(entry)

    def check_processed_analysis_file(self, number_of_lines):
        with open(self.processed_analyses_file, 'r') as f:
            lines = f.readlines()
            self.assertEqual(len(lines), number_of_lines)

    def test_download_analyses(self):
        data = self.get_processed_files_data()
        self.create_processed_analysis_file(data)
        download_analyses(project=self.project, num_analyses=self.num_analyses_to_download,
                          processed_analyses_file=self.processed_analyses_file,
                          download_target_dir=self.download_target_dir)
        vcf_files = glob.glob(f"{self.download_target_dir}/*.vcf")
        self.assertEqual(self.num_analyses_to_download, len(vcf_files))
        self.check_processed_analysis_file(len(data) + self.num_analyses_to_download)
