import re
import os
import glob
import shutil
from os import path
import numpy as np
import pandas as pd
import configparser
from datetime import datetime


# Read the config file for various attributes and then proceed further
def read_config():
    config = 'C:\\Users\\gurme\\Desktop\\ManulifeChallenge\\filemerge.config'
    config_parser = configparser.ConfigParser()
    config_parser.read(config)

    src_files_path = config_parser.get('FileInfo', 'src_file')
    output_file = config_parser.get('FileInfo', 'output_file')
    combined_file_cols = config_parser.get('output_file_cols', 'combined_file_cols')
    existing_combined_file_ops_del = config_parser.get('file_ops', 'del_file')
    existing_combined_file_ops_archive = config_parser.get('file_ops', 'archive_file')

    all_files = glob.glob(src_files_path + "/*.csv")
    final_file = os.path.join(src_files_path, output_file)
    return combined_file_cols, existing_combined_file_ops_del, \
           existing_combined_file_ops_archive, all_files, final_file, src_files_path


# Start files' processing after reading config file.
def start_processing():
    combined_file_cols, existing_combined_file_ops_del, \
    existing_combined_file_ops_archive, all_files, final_file, src_files_path = read_config()

    # Check if source folder has any files or not
    files_present_flag = chk_files_present(all_files)

    if files_present_flag == 0:

        # Check if files coming in for processing are of csv type or not.
        validator_flag = check_file_type(all_files)

        # 0 is True value and 1 is False value check
        if validator_flag == 0:
            # Below clause will not count 'Combined.csv' file as input file,
            # if it is present in the source folder already
            if final_file in all_files:
                all_files.remove(final_file)
                read_files(all_files, final_file, combined_file_cols, existing_combined_file_ops_archive,
                       existing_combined_file_ops_del, src_files_path)
            else:
                read_files(all_files, final_file, combined_file_cols, existing_combined_file_ops_archive,
                           existing_combined_file_ops_del, src_files_path)
        elif validator_flag == 1:
            print('The input file(s) is/are not csv types, please check the set of input files.')
    else:
        print('There are no files of csv type present.')


# Check if files are present in source folder or not
def chk_files_present(all_files):
    if len(all_files) > 0:
        # print('Files are present to be processed.')
        return 0
    else:
        # print('There are no files present to be processed.')
        return 1


# Check if all files coming in for processing are of csv type or not.
def check_file_type(all_files):
    if all('.csv' in file_name for file_name in all_files):
        return 0
    else:
        return 1


# Delete the existing Combined.csv if it is already existing based on flag coming from config file
def del_if_any_combined_file_exist(final_file):
    if path.exists(final_file):
        try:
            print('Deleting existing Combined.csv file.')
            os.remove(final_file)
        except OSError as osError:
            print('There is an exception with file removal operation. The error is: ' + str(osError))


# Archive the existing Combined.csv if it is already existing based on flag coming from config file
def archive_existing_combined_file(final_file, src_files_path):
    archive_file_folder = src_files_path + "\\" + "Archived_Combined_Files\\"

    # final_file is the file that we either want to delete or archive - combined.csv, if already existing
    if path.exists(final_file):
        if path.exists(archive_file_folder):
            shutil.move(final_file, archive_file_folder + "\\" + datetime.today().strftime('%Y-%m-%d-%H_%M_%S') + "_"
                        + "Combined.csv")
        else:
            try:
                os.makedirs(archive_file_folder)
                shutil.move(final_file, archive_file_folder + "\\" + datetime.today().strftime('%Y-%m-%d-%H_%M_%S')
                            + "_" + "Combined.csv")
            except OSError as osError:
                print('There is an exception with archiving operation. The error is: ' + str(osError))


# Read the incoming source files and then generate the Combined.csv file

# Assumption ======  If there is a csv file that is not of 'asia prod' type or 'na preview' type,
# the environment value should be set 'Not Applicable' iff the file has the same structure
# that is expected for data transformation.

def read_files(all_files, final_file, combined_file_cols, existing_combined_file_ops_archive,
               existing_combined_file_ops_del, src_files_path):
    # 0 value means true and 1 value means false

    if existing_combined_file_ops_del == '0':
        # Delete if any existing combined.csv exists in the same path
        del_if_any_combined_file_exist(final_file)

    if existing_combined_file_ops_archive == '0':
        # Archive the existing combined.csv file to an archive folder and
        # rename combined.csv with date when archiving is happening
        archive_existing_combined_file(final_file, src_files_path)

    # Below code will start reading the file
    clean_df_list = []

    for filename in all_files:
        df = pd.read_csv(filename, index_col=None, header=0)
        df = df[['Source IP']]
        df.drop_duplicates(subset='Source IP', inplace=True)

        # Below function will validate the data coming in 'Source IP' column in every incoming file.
        input_ip_data_chk = chk_SourceIP_column(df)

        if input_ip_data_chk == 0:

            filename = filename.lower()

            if 'asia prod' in filename:
                df.insert(1, 'Environment', 'Asia Prod')
            elif 'na preview' in filename:
                df.insert(1, 'Environment', 'NA Preview')
            else:
                df.insert(1, 'Environment', 'Not Applicable')

            clean_df_list.append(df)

        else:
            print('Dataset from input files cannot be made because SourceIP in input file is not matching. '
                  'The incorrect file is: ' + str(filename))

    combined_ds_frame = pd.concat(clean_df_list, axis=0, ignore_index=True)

    combined_df_headers = combined_ds_frame.columns.values.tolist()
    config_cols_list = combined_file_cols.split(",")

    if combined_df_headers == config_cols_list:
        # Check if data type in Combined.csv source ip column is only certain pattern numbers and
        # Environment column is only containing certain applicable values

        ip_data_chk = chk_output_dataset(combined_ds_frame)

        if ip_data_chk == 0:
            combined_ds_frame.to_csv(final_file, index=False)
            print('Combined.csv file has been generated.')
        else:
            print('Data generated for Combined.csv is not appropriate. Hence, csv file will nto be generated.')
    else:
        print('Dataset to be generated for Combined.csv is not having expected set of columns. Please recheck.')


# Validate SourceIP column for dataset
def chk_SourceIP_column(dataset_to_be_validated):
    mismatch_count = 0
    ipregex = "^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})$"
    ip_list = list(dataset_to_be_validated['Source IP'])

    # This will check if all Source IP values are matching the pattern.
    # If not mismatch_count will be increased by 1 every time.
    for per_ip in ip_list:
        if re.match(ipregex, per_ip):
            pass
        else:
            mismatch_count = mismatch_count + 1

    if mismatch_count == 0:
        return 0
    else:
        return 1

# Validate the Output dataset
def chk_output_dataset(combined_ds_frame):
    valid_list_of_env = {'Asia Prod', 'NA Preview', 'Not Applicable'}
    mismatch_count = chk_SourceIP_column(combined_ds_frame)

    # Second check is to confirm that the Environment column is getting the right set of values.
    # Because valid values are 'Asia Prod', 'NA Preview', 'Not Applicable', if any other type of value is found,
    # it will cause mismatch_count to be increased by 1 again.
    if combined_ds_frame['Environment'].dtype == np.object_:
        list_of_env = set(combined_ds_frame['Environment'])

        if list_of_env.issubset(valid_list_of_env) or list_of_env == valid_list_of_env:
            pass
        else:
            mismatch_count = mismatch_count + 1
    else:
        print('Combined Dataframe "Environment" column is not of required type. Please check.')
        mismatch_count = mismatch_count + 1

    if mismatch_count == 0:
        return 0
    else:
        return 1


start_processing()
