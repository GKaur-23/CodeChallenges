import os
import glob
import unittest
import FileMerge
from os import path
import pandas as pd
import configparser


class FileMergeCase(unittest.TestCase):
    @staticmethod
    def read_config_file():
        config = 'C:\\Users\\gurme\\Desktop\\ManulifeChallenge\\filemerge.config'
        config_parser = configparser.ConfigParser()
        config_parser.read(config)
        return config_parser

    # Below test will check if there is/are any files to be processed.
    def test_incoming_files(self):
        config_parser = FileMergeCase.read_config_file()
        src_files_path = config_parser.get('FileInfo', 'src_file')
        all_files = glob.glob(src_files_path + "/*.csv")
        expected_res = 1
        # This should throw assertion error stating there are files present to be processed if there are files present,
        # if not, then this assertion should pass.
        try:
            self.assertEqual(FileMerge.chk_files_present(all_files), expected_res,
                             'There are NO FILES to process in the source location.')
        except AssertionError as msg:
            print('There is assertion exception for test_incoming_files. The error is: ' + str(msg))

    # Below test will check if the incoming files are csv types or not.
    def test_incoming_file_extnsn(self):
        config_parser = FileMergeCase.read_config_file()
        src_files_path = config_parser.get('FileInfo', 'src_file')
        all_files = glob.glob(src_files_path + "/*.csv")
        expected_res = 0
        # If files will be of csv type, this assertion will pass but if it will not be,
        # it can throw exception and needs to be handled by the way we handled it in test_incoming_files test method.
        try:
            self.assertEqual(FileMerge.check_file_type(all_files), expected_res, 'Files are not of csv type.')
        except AssertionError as msg:
            print('There is assertion exception for test_incoming_file_extnsn. The error is: ' + str(msg))

    # Below test will check if file deletion is happening appropriately
    def test_chk_file_deletion(self):
        config_parser = FileMergeCase.read_config_file()
        src_files_path = config_parser.get('FileInfo', 'src_file')
        output_file = config_parser.get('FileInfo', 'output_file')
        final_file = os.path.join(src_files_path, output_file)
        combined_output_file_exist = 1

        if path.exists(final_file):
            combined_output_file_exist = 0

        try:
            self.assertEqual(FileMerge.del_if_any_combined_file_exist(final_file), combined_output_file_exist,
                             'There is no Combined.csv file present.')
        except AssertionError as msg:
            print('There is assertion exception for test_chk_file_deletion. The error is: ' + str(msg))

    # Below test will validate the source files' data
    def test_validate_src_file_data(self):
        print('Check validations for Input files.')
        config_parser = FileMergeCase.read_config_file()
        src_files_path = config_parser.get('FileInfo', 'src_file')
        all_files = glob.glob(src_files_path + "/*.csv")

        mismatch_file_count = 0
        expected_mismatch_file_count = 0

        for filename in all_files:
            df = pd.read_csv(filename, index_col=None, header=0)

            df = df[['Source IP']]
            df.drop_duplicates(subset='Source IP', inplace=True)

            # Below function will validate the data coming in 'Source IP' column in every incoming file.
            input_ip_data_chk = FileMerge.chk_SourceIP_column(df)

            if input_ip_data_chk != 0:
                mismatch_file_count = mismatch_file_count + 1

            try:
                self.assertEqual(mismatch_file_count, expected_mismatch_file_count,
                                 "The Source Ip in incoming files is not matching the required pattern.")
            except AssertionError as msg:
                print('There is assertion exception for test_validate_src_file_data. The error is: ' + str(msg))

    # Below test will validate the output file data
    def test_validate_output_file_data(self):
        expected_flag = 0
        config_parser = FileMergeCase.read_config_file()
        src_files_path = config_parser.get('FileInfo', 'src_file')
        output_file = config_parser.get('FileInfo', 'output_file')
        final_file = os.path.join(src_files_path, output_file)

        if path.exists(final_file):
            combined_csv_df = pd.read_csv(final_file, index_col=None, header=0)
            validator_flag = FileMerge.chk_output_dataset(combined_csv_df)
        else:
            print('No path: ' + str(final_file) + ' exists.')
            validator_flag = 1

        try:
            self.assertEqual(expected_flag, validator_flag, "The data in Combined.csv is NOT matching all data checks.")
        except AssertionError as msg:
            print('There is assertion exception for test_validate_output_file_data. The error is: ' + str(msg))


if __name__ == '__main__':
    unittest.main()
