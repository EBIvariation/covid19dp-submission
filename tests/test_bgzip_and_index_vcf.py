import glob
import os
import shutil
from unittest import TestCase

from covid19dp_submission import ROOT_DIR
from covid19dp_submission.steps.bgzip_and_index_vcf import bgzip_and_index


class TestBGZipAndIndex(TestCase):
    resources_folder = os.path.join(ROOT_DIR, 'tests', 'resources')
    download_folder = os.path.join(resources_folder, 'download_snapshot')
    download_target_dir = os.path.join(download_folder, '30_eva_valid', '2021_07_23_test_snapshot')

    def setUp(self) -> None:
        shutil.rmtree(self.download_folder, ignore_errors=True)

    def tearDown(self) -> None:
        shutil.rmtree(self.download_folder, ignore_errors=True)

    def download_test_files(self):
        os.makedirs(self.download_target_dir)
        shutil.copy(os.path.join(self.resources_folder, 'vcf_files', 'file1.vcf'), self.download_target_dir)
        return self.download_target_dir

    def test_bgzip_and_index(self):
        download_dir = self.download_test_files()
        vcf_files = glob.glob(f"{download_dir}/*.vcf")
        output_file = f"{vcf_files[0]}.gz"
        bgzip_and_index(vcf_file=vcf_files[0], output_file=output_file, bcftools_binary="bcftools")
        self.assertTrue(os.path.exists(output_file))
        self.assertEqual(1, len(glob.glob(f"{output_file}.csi")))
