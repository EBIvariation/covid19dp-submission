import glob
import os
import shutil
from unittest import TestCase
from unittest.mock import patch

from covid19dp_submission import ROOT_DIR
from covid19dp_submission.download_analyses import download_analyses, download_files_via_aspera


class TestDownloadSnapshot(TestCase):
    resources_folder = os.path.join(ROOT_DIR, 'tests', 'resources')
    toplevel_download_folder = os.path.join(resources_folder, 'download_analyses')
    download_target_dir = os.path.join(toplevel_download_folder, '30_eva_valid')
    processed_analyses_file = os.path.join(toplevel_download_folder, 'processed_analyses_file.txt')
    project = 'PRJEB45554'
    num_analyses_to_download = 1

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
        ascp_bin = os.path.expanduser("~/.aspera/connect/bin/ascp")
        aspera_id_dsa_key = os.path.expanduser("~/.aspera/connect/etc/asperaweb_id_dsa.openssh")
        download_analyses(project=self.project, num_analyses=self.num_analyses_to_download,
                          processed_analyses_file=self.processed_analyses_file,
                          download_target_dir=self.download_target_dir, ascp=ascp_bin,
                          aspera_id_dsa=aspera_id_dsa_key, batch_size=100)
        vcf_files = glob.glob(f"{self.download_target_dir}/*.vcf")
        self.assertEqual(self.num_analyses_to_download, len(vcf_files))
        self.check_processed_analysis_file(len(data) + self.num_analyses_to_download)


class TestDownloadAnalysisENA(TestCase):
    resources_folder = os.path.join(ROOT_DIR, 'tests', 'resources')
    download_target_dir = os.path.join(resources_folder, 'download_snapshot')
    processed_analyses_file = os.path.join(download_target_dir, 'processed_analyses.csv')

    def tearDown(self) -> None:
        if os.path.exists(self.processed_analyses_file):
            os.remove(self.processed_analyses_file)

    def test_download_files_via_aspera(self):
        analyses_array = [
            {'run_ref': f'rr{i}', 'analysis_accession': f'acc{i}', 'submitted_ftp': f'ftp.ebi.ac.uk/acc{i}/rr{i}.vcf.gz ',
             'submitted_aspera': f'asperap.ebi.ac.uk/acc{i}/rr{i}.vcf.gz'} for i in range(1, 9)
        ]
        with patch('covid19dp_submission.download_analyses.run_command_with_output') as mock_run:
            download_files_via_aspera(analyses_array, self.download_target_dir, self.processed_analyses_file,
                                      'ascp', 'aspera_id_dsa', batch_size=5)
        # 2 batches of 5 to get 8 files
        assert mock_run.call_count == 2
        with open(self.processed_analyses_file) as open_file:
            lines = open_file.readlines()
            assert len(lines) == 8
