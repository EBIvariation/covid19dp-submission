import glob
import os
import shutil

from covid19dp_submission.steps.download_snapshot import download_snapshot
from covid19dp_submission.steps.bgzip_and_index_vcf import bgzip_and_index
from covid19dp_submission import ROOT_DIR
from unittest import TestCase


class TestBGZipAndIndex(TestCase):
    resources_folder = os.path.join(ROOT_DIR, 'tests', 'resources')
    download_folder = os.path.join(resources_folder, 'download_snapshot')
    download_target_dir = os.path.join(download_folder, '30_eva_valid', '2021_07_23_test_snapshot')
    download_url = "file:///" + os.path.join(resources_folder, 'vcf_files', '2021_07_23_test_snapshot.tar.gz')

    def setUp(self) -> None:
        shutil.rmtree(self.download_folder, ignore_errors=True)

    def tearDown(self) -> None:
        shutil.rmtree(self.download_folder, ignore_errors=True)

    def test_bgzip_and_index(self):
        download_dir = download_snapshot(download_url=self.download_url, snapshot_name=None,
                                         download_target_dir=self.download_target_dir)
        vcf_files = glob.glob(f"{download_dir}/*.vcf.gz")
        bgzip_and_index(vcf_file=vcf_files[0], bcftools_binary="bcftools")
        self.assertTrue(os.path.exists(vcf_files[0]))
        self.assertEqual(1, len(glob.glob(f"{vcf_files[0]}.csi")))
