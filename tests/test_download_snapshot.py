import glob
import os
import shutil

from covid19dp_submission.steps.download_snapshot import download_snapshot
from covid19dp_submission import ROOT_DIR
from unittest import TestCase


class TestDownloadSnapshot(TestCase):
    resources_folder = os.path.join(ROOT_DIR, 'tests', 'resources')
    toplevel_download_folder = os.path.join(resources_folder, 'download_snapshot')
    download_target_dir = os.path.join(toplevel_download_folder, '30_eva_valid', '2021_07_23_test_snapshot')
    download_url = "file:///" + os.path.join(resources_folder, 'vcf_files', '2021_07_23_test_snapshot.tar.gz')

    def setUp(self) -> None:
        shutil.rmtree(self.toplevel_download_folder, ignore_errors=True)

    def tearDown(self) -> None:
        shutil.rmtree(self.toplevel_download_folder, ignore_errors=True)

    def download_snapshot_archive(self):
        return download_snapshot(download_url=self.download_url, snapshot_name=None,
                                 download_target_dir=self.download_target_dir)

    def test_download_snapshot(self):
        actual_download_dir = self.download_snapshot_archive()
        self.assertEqual(self.download_target_dir, actual_download_dir)
        vcf_files = glob.glob(f"{actual_download_dir}/*.vcf.gz")
        self.assertEqual(5, len(vcf_files))

        original_snapshot_file = glob.glob(f"{actual_download_dir}/*.tar.gz")
        self.assertEqual(0, len(original_snapshot_file))

    def test_redownload_snapshot(self):
        self.download_snapshot_archive()
        with self.assertRaises(FileExistsError) as snapshot_exists_exception:
            self.download_snapshot_archive()
        self.assertEqual(snapshot_exists_exception.exception.args[0],
                         f"FAIL: Snapshot already downloaded to target directory: "
                         f"{self.download_target_dir}. "
                         f"Please delete that directory to re-download.")
