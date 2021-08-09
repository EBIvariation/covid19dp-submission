import os
import shutil
import subprocess

from covid19dp_submission.steps.run_asm_checker import run_asm_checker, should_skip_asm_check
from covid19dp_submission import ROOT_DIR
from ebi_eva_common_pyutils.command_utils import run_command_with_output
from unittest import TestCase


class TestRunAsmChecker(TestCase):
    resources_folder = os.path.join(ROOT_DIR, 'tests', 'resources')
    vcf_files_folder = os.path.join(resources_folder, 'vcf_files')
    asm_check_test_run_folder = os.path.join(resources_folder, 'asm_check_run')
    assembly_report_url = os.path.join(resources_folder, 
                                       'GCA_009858895.3_ASM985889v3_assembly_report.txt')
    fasta_file = os.path.join(resources_folder, 'GCA_009858895.3_ASM985889v3_genomic.fna')

    def setUp(self) -> None:
        shutil.rmtree(self.asm_check_test_run_folder, ignore_errors=True)
        os.makedirs(self.asm_check_test_run_folder)

    def tearDown(self) -> None:
        shutil.rmtree(self.asm_check_test_run_folder, ignore_errors=True)

    def test_should_skip_asm_check(self):
        file_with_no_variants_only_headers = os.path.join(self.vcf_files_folder,
                                                          'file_with_no_variants_only_headers.vcf.gz')
        self.assertTrue(should_skip_asm_check(file_with_no_variants_only_headers))

    def test_asm_check_successful(self):
        file_that_should_not_generate_error = f"{self.vcf_files_folder}/file1.vcf.gz"
        run_asm_checker(vcf_file=file_that_should_not_generate_error,
                        assembly_checker_binary="vcf_assembly_checker_linux",
                        assembly_report=self.assembly_report_url, assembly_fasta=self.fasta_file,
                        output_dir=self.asm_check_test_run_folder)
        asm_check_percentage_match = run_command_with_output(
            f"Finding assembly check percentage match for {file_that_should_not_generate_error}...",
            f'grep -E -o "[0-9]+[.0-9]*%" '
            f'{self.asm_check_test_run_folder}/'
            f'{os.path.basename(file_that_should_not_generate_error)}.assembly_check.log',
            return_process_output=True).strip()
        self.assertEqual("100%", asm_check_percentage_match)

    def test_asm_check_failed(self):
        # File has 3 variants out of 15 (20%) that will not match the reference assembly
        file_that_should_generate_error = f"{self.vcf_files_folder}/file_that_will_fail_asm_check.vcf.gz"
        with self.assertRaises(subprocess.CalledProcessError) as exit_exception:
            run_asm_checker(vcf_file=file_that_should_generate_error,
                            assembly_checker_binary="vcf_assembly_checker_linux",
                            assembly_report=self.assembly_report_url, assembly_fasta=self.fasta_file,
                            output_dir=self.asm_check_test_run_folder)
        asm_check_percentage_match = run_command_with_output(
            f"Finding assembly check percentage match for {file_that_should_generate_error}...",
            f'grep -E -o "[0-9]+[.0-9]*%" '
            f'{self.asm_check_test_run_folder}/'
            f'{os.path.basename(file_that_should_generate_error)}.assembly_check.log',
            return_process_output=True).strip()
        self.assertEqual("80%", asm_check_percentage_match)
