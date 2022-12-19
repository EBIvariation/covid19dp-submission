import glob
import os
import shutil
from unittest import TestCase
from unittest.mock import patch

from covid19dp_submission import ROOT_DIR
from covid19dp_submission.download_analyses import download_analyses, download_files_via_aspera, UnfinishedBatchError, \
    add_to_ignored_file, get_analyses_to_process


def touch(f):
    open(f, 'w').close()


class TestDownloadAnalysis(TestCase):
    resources_folder = os.path.join(ROOT_DIR, 'tests', 'resources')
    toplevel_download_folder = os.path.join(resources_folder, 'download_analyses')

    def setUp(self) -> None:
        shutil.rmtree(self.toplevel_download_folder, ignore_errors=True)
        os.makedirs(self.toplevel_download_folder)

    def tearDown(self) -> None:
        shutil.rmtree(self.toplevel_download_folder, ignore_errors=True)

    def _test_get_analyses_to_process(self, ignored_analysis=None, taxonomy=2697049):
        processed_analyses_file = os.path.join(self.toplevel_download_folder, 'processed_analyses.csv')
        ignored_analysis_file = os.path.join(self.toplevel_download_folder, 'ignored_analysis.csv')
        if ignored_analysis:
            with open(ignored_analysis_file, 'w') as open_file:
                for a in ignored_analysis:
                    open_file.write(a + ',')
        analyses = get_analyses_to_process(
            project='PRJEB45554', num_analyses=10, total_analyses=100,
            processed_analyses_file=processed_analyses_file, ignored_analysis_file=ignored_analysis_file,
            accepted_taxonomies=[taxonomy]
        )
        return analyses


    def test_get_analyses_to_process(self):
        analyses = self._test_get_analyses_to_process()
        assert [a.get('analysis_accession') for a in analyses] == [
            'ERZ10000001', 'ERZ10000003', 'ERZ10000004', 'ERZ10000009', 'ERZ10000010',
            'ERZ10000014', 'ERZ10000017', 'ERZ10000018', 'ERZ10000021', 'ERZ10000023'
        ]

    def test_get_analyses_to_process_taxo_filter(self):
        analyses = self._test_get_analyses_to_process(taxonomy=999999)
        assert [a.get('analysis_accession') for a in analyses] == []

    def test_get_analyses_to_process_ignored_filter(self):
        analyses = self._test_get_analyses_to_process(ignored_analysis=['ERZ10000001', 'ERZ10000003', 'ERZ10000004', 'ERZ10000009', 'ERZ10000010'])
        assert [a.get('analysis_accession') for a in analyses] == [
            'ERZ10000003', 'ERZ10000004', 'ERZ10000009', 'ERZ10000010', 'ERZ10000014',
            'ERZ10000017', 'ERZ10000018', 'ERZ10000021', 'ERZ10000023', 'ERZ10000025'
        ]

    def test_add_to_ignored_file(self):
        analysis_to_ignore = [
            {'analysis_accession': 'accession1', 'submitted_ftp': 'ftp.example.com/accession1/vcf_file1.vcf.gz'},
            {'analysis_accession': 'accession2', 'submitted_ftp': 'ftp.example.com/accession2/vcf_file1.vcf.gz'}
        ]
        ignored_analysis_file = os.path.join(self.toplevel_download_folder, 'analysis_to_ignore.csv')
        add_to_ignored_file(analyses_array=analysis_to_ignore, ignored_analysis_file=ignored_analysis_file)
        with open(ignored_analysis_file) as open_file:
            lines = open_file.readlines()
        assert len(lines) == 2
        add_to_ignored_file(analyses_array=analysis_to_ignore, ignored_analysis_file=ignored_analysis_file)
        with open(ignored_analysis_file) as open_file:
            lines = open_file.readlines()
        assert len(lines) == 4


class TestDownloadSnapshot(TestCase):
    resources_folder = os.path.join(ROOT_DIR, 'tests', 'resources')
    toplevel_download_folder = os.path.join(resources_folder, 'download_analyses')
    download_target_dir = os.path.join(toplevel_download_folder, '30_eva_valid')
    processed_analyses_file = os.path.join(toplevel_download_folder, 'processed_analyses_file.txt')
    ignored_analyses_file = os.path.join(toplevel_download_folder, 'ignored_analyses_file.txt')
    project = 'PRJEB45554'
    accepted_taxonomies = [2697049]
    num_analyses_to_download = 1

    def setUp(self) -> None:
        shutil.rmtree(self.toplevel_download_folder, ignore_errors=True)
        os.makedirs(self.download_target_dir)
        with open(self.ignored_analyses_file, 'w'):
            pass

    def tearDown(self) -> None:
        shutil.rmtree(self.toplevel_download_folder, ignore_errors=True)

    def get_processed_files_data(self):
        return ["ERZ3372540,ftp.sra.ebi.ac.uk/vol1/ERZ337/ERZ3372540/SRR15239121.vcf\n",
                "ERZ3372549,ftp.sra.ebi.ac.uk/vol1/ERZ337/ERZ3372549/ERR6259542.vcf\n",
                "ERZ3372550,ftp.sra.ebi.ac.uk/vol1/ERZ337/ERZ3372550/ERR6259546.vcf\n",
                "ERZ3372551,ftp.sra.ebi.ac.uk/vol1/ERZ337/ERZ3372551/ERR6259557.vcf\n",
                "ERZ3400189,ftp.sra.ebi.ac.uk/vol1/ERZ340/ERZ3400189/SRR15239121.vcf\n"
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
        ascp_bin = "ascp"
        aspera_id_dsa_key = os.environ['ASPERA_ID_DSA']
        download_analyses(project=self.project, num_analyses=self.num_analyses_to_download,
                          processed_analyses_file=self.processed_analyses_file,
                          ignored_analysis_file=self.ignored_analyses_file,
                          accepted_taxonomies=self.accepted_taxonomies,
                          download_target_dir=self.download_target_dir, ascp=ascp_bin,
                          aspera_id_dsa=aspera_id_dsa_key, batch_size=100)
        vcf_files = glob.glob(f"{self.download_target_dir}/*.vcf") + glob.glob(f"{self.download_target_dir}/*.vcf.gz")
        self.assertEqual(self.num_analyses_to_download, len(vcf_files))
        self.check_processed_analysis_file(len(data) + self.num_analyses_to_download)


class TestDownloadAnalysisENA(TestCase):
    resources_folder = os.path.join(ROOT_DIR, 'tests', 'resources')
    download_target_dir = os.path.join(resources_folder, 'download_analyses')
    processed_analyses_file = os.path.join(download_target_dir, 'processed_analyses.csv')

    def setUp(self) -> None:
        os.makedirs(self.download_target_dir, exist_ok=True)

    def tearDown(self) -> None:
        shutil.rmtree(self.download_target_dir, ignore_errors=True)

    def test_download_files_via_aspera(self):
        analyses_array = [
            {'run_ref': f'rr{i}', 'analysis_accession': f'acc{i}', 'submitted_ftp': f'ftp.ebi.ac.uk/acc{i}/rr{i}.vcf.gz',
             'submitted_aspera': f'asperap.ebi.ac.uk/acc{i}/rr{i}.vcf.gz'} for i in range(1, 9)
        ]
        with patch('covid19dp_submission.download_analyses.run_command_with_output') as mock_run:
            # create the expected output files so that the download do not crash
            for analysis in analyses_array:
                expected_file = os.path.join(self.download_target_dir, os.path.basename(analysis['submitted_aspera']))
                touch(expected_file)
            download_files_via_aspera(analyses_array, self.download_target_dir, self.processed_analyses_file,
                                      'ascp', 'aspera_id_dsa', batch_size=5)
        # 2 batches of 5 to get 8 files
        assert mock_run.call_count == 2
        with open(self.processed_analyses_file) as open_file:
            lines = open_file.readlines()
            assert len(lines) == 8

    def test_retry_download_files_via_aspera(self):
        analyses_array = [
            {'run_ref': f'rr{i}', 'analysis_accession': f'acc{i}', 'submitted_ftp': f'ftp.ebi.ac.uk/acc{i}/rr{i}.vcf.gz',
             'submitted_aspera': f'asperap.ebi.ac.uk/acc{i}/rr{i}.vcf.gz'} for i in range(1, 9)
        ]
        with patch('covid19dp_submission.download_analyses.run_command_with_output') as mock_run:
            # create some of the expected output files so that the download retries
            for analysis in analyses_array[:-1]:
                expected_file = os.path.join(self.download_target_dir, os.path.basename(analysis['submitted_aspera']))
                touch(expected_file)
            with self.assertRaises(UnfinishedBatchError):
                download_files_via_aspera(analyses_array, self.download_target_dir, self.processed_analyses_file,
                                          'ascp', 'aspera_id_dsa', batch_size=5)
        # 2 batches of 5 in first try then 3 more in subsequent tries = 5
        assert mock_run.call_count == 5
        with open(self.processed_analyses_file) as open_file:
            lines = open_file.readlines()
            assert len(lines) == 7
