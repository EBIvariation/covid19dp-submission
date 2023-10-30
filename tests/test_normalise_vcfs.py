import glob
import os
import shutil
from unittest import TestCase

from covid19dp_submission import ROOT_DIR
from covid19dp_submission.steps.normalise_vcfs import normalise_all


class TestNormaliseVCFs(TestCase):
    resources_folder = os.path.join(ROOT_DIR, 'tests', 'resources')
    download_folder = os.path.join(resources_folder, 'download_snapshot')
    download_target_dir = os.path.join(download_folder, '30_eva_valid', '2021_07_23_test_snapshot')
    output_dir = os.path.join(download_folder, 'normalised_vcfs')
    refseq_fasta_file = os.path.join(resources_folder, 'sars_cov2_refseq_fasta.fna')

    def setUp(self) -> None:
        shutil.rmtree(self.download_folder, ignore_errors=True)

    def tearDown(self) -> None:
        shutil.rmtree(self.download_folder, ignore_errors=True)

    def download_test_files(self):
        os.makedirs(self.download_target_dir)
        shutil.copy(os.path.join(self.resources_folder, 'vcf_files', 'file_with_unnormalised_variants.vcf.gz'),
                    self.download_target_dir)
        shutil.copy(os.path.join(self.resources_folder, 'vcf_files', 'file_with_unnormalised_variants.vcf.gz.csi'),
                    self.download_target_dir)
        return self.download_target_dir

    def test_normalise_vcfs(self):
        download_dir = self.download_test_files()
        vcf_files = glob.glob(f"{download_dir}/*.vcf.gz")
        output_file = f"{self.output_dir}/{os.path.basename(vcf_files[0])}"
        normalise_all(download_dir, vcf_files=vcf_files, output_dir=self.output_dir, bcftools_binary="bcftools",
                      refseq_fasta_file=self.refseq_fasta_file)
        self.assertTrue(os.path.exists(output_file))
        self.assertEqual(1, len(glob.glob(f"{output_file}.csi")))
