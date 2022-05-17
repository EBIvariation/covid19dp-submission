from unittest import TestCase

from covid19dp_submission.steps.vcf_vertical_concat.run_vcf_vertical_concat_pipeline import get_concat_result_file_name


class TestVcfVerticalConcat(TestCase):

    def test_get_concat_result_file_name(self):
        concat_processing_dir = ''
        total_number_of_vcf_files = 0
        concat_chunk_size = 100

        result = get_concat_result_file_name(concat_processing_dir, total_number_of_vcf_files, concat_chunk_size)
        print(result)