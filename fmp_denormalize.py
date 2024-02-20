import os, sys
import zipfile
import pandas as pd
import argparse
import pathlib
from tqdm import tqdm

def read_csv(input_file):
    data = pd.read_csv(input_file, dtype=str).fillna('')
    print(f'total rows in {input_file}: {len(data)}')
    # discard any rows where Organization ID is empty
    data = data[data['Organization ID'] != '']
    print(f'total rows in {input_file} after removing empty Organization ID: {len(data)}')
    return data

def read_csvs_from_dir(input_dir, expected_filenames):
    # Check that all expected files are present
    for filename in expected_filenames:
        if not os.path.exists(f'{input_dir}/{filename}'):
            print(f'File {filename} not found in input directory {input_dir}')
            exit(1)
    # Read in data
    data = {}
    for filename in expected_filenames:
        data[filename.split('.')[0]] = read_csv(f'{input_dir}/{filename}')
    return data

def read_csvs_from_zip(input_zip, expected_filenames):
    # Check that all expected files are present
    with zipfile.ZipFile(input_zip, 'r') as zip_ref:
        for filename in expected_filenames:
            if filename not in zip_ref.namelist():
                print(f'File {filename} not found in input zip {input_zip}')
                exit(1)
    # Read in data
    data = {}
    with zipfile.ZipFile(input_zip, 'r') as zip_ref:
        for filename in expected_filenames:
            with zip_ref.open(filename) as f:
                data[filename.split('.')[0]] = read_csv(f)
    return data

def join_unique_values(values):
    # This function will be called by the agg method for the groupby operation in handle_duplicates
    return '|'.join(set(values))

def join_data(main_data, data_to_join, join_on, suffix=None):
    if suffix is None:
        raise ValueError('suffix must be specified')
    return main_data.merge(data_to_join, how='left', on=join_on, suffixes=(None, suffix) )

def handle_duplicates(df, join_on, unique_only=True):
    # print(f'columns in df: {df.columns}')
    # remove completely duplicate rows
    df = df.drop_duplicates()
    # combine rows with the same join_on value
    if unique_only:
        df = df.groupby(join_on).agg(join_unique_values).reset_index()
    else:
        df = df.groupby(join_on).agg(lambda x: '|'.join(x)).reset_index()
    # print(f'columns in df after groupby: {df.columns}')
    return df

def save_data_to_csv(data, output_file):
    data.to_csv(output_file, index=False)
    #for testing, save a version of the data with only the first 500 rows
    data.head(500).to_csv(output_file.replace('.csv', '_head500.csv'), index=False)

def prep_limit_orgs(orgs_to_include):
    # The format of the org IDs coming from FMP is different than in other places
    # If the limit_orgs file has IDs in the format ```HH######```, we need to convert to ```HH_######```

    # If the first 2 characters are HH and the 3rd character is a digit, replace the 3rd character with an underscore
    for i in range(len(orgs_to_include)):
        if orgs_to_include[i][:2] == 'HH' and orgs_to_include[i][2].isdigit():
            orgs_to_include[i] = orgs_to_include[i][:2] + '_' + orgs_to_include[i][2:]
    return orgs_to_include



if __name__ == '__main__':
    # Parse command line arguments
    try:
        parser = argparse.ArgumentParser(description='test')#"""
            # Denormalize data from the FileMaker Pro database export into a single CSV file.
            # Input is either a directory containing the 7 CSV files or a zip file containing the 7 CSV files.
            # The expected filenames are: alternative_name.csv, folders.csv, locations.csv, members.csv, related_collections.csv, sources.csv, subjects.csv
            # If an output csv file is specified, the denormalized data will be written to that file.
            # If a directory is specified, the denormalized data will be written to a file called fmp_denormalized.csv in that directory.
            # """)
        parser.add_argument('--input_dir', help='Path to directory containing all 7 FMP csv files. Must specify either input_dir or input_zip, but not both', required=False)
        parser.add_argument('--input_zip', help='Path to zip file containing all 7 FMP csv files. Must specify either input_dir or input_zip, but not both', required=False)
        parser.add_argument('--output_path', help='Output csv or directory for denormalized data', required=True)
        parser.add_argument('--limit_orgs', help='Limit the orgs to include in the output. Specify a path to a txt or csv file containing a list of org IDs to include', required=False)
        args = parser.parse_args()
        

        # Make sure exactly one of input_dir or input_zip is specified
        if args.input_dir is None and args.input_zip is None:
            print('Must specify either input_dir or input_zip')
            exit(1)
        if args.input_dir is not None and args.input_zip is not None:
            print('Must specify either input_dir or input_zip, but not both')
            exit(1)

        # Determine if the output is a directory or a file
        if os.path.isdir(args.output_path):
            output_dir = args.output_path
            output_file = f'{output_dir}/fmp_denormalized.csv'
        else:
            output_dir = None
            output_file = args.output_path

        # Read in data
        expected_filenames = ['alternative_name.csv',
                              'folders.csv',
                              'locations.csv',
                              'members.csv',
                              'related_collections.csv',
                              'sources.csv',
                              'subjects.csv'
                              ]
        
        if args.input_dir is not None:
            data = read_csvs_from_dir(args.input_dir, expected_filenames)
        else:
            data = read_csvs_from_zip(args.input_zip, expected_filenames)

        print('Data read in successfully')
        print(f'folder data: {data["folders"].head(20)}')

        # #print any rows in folders with float values
        # for index, row in data['folders'].iterrows():
        #     for column in data['folders'].columns:
        #         if 'float' in str(data['folders'][column].dtype):
        #             print(f'row {index} column {column} value {row[column]}')

        # If limit_orgs is specified, read in the list of orgs to include
        if args.limit_orgs is not None:
            with open(args.limit_orgs, 'r') as f:
                    orgs_to_include = f.read().splitlines()
            orgs_to_include = prep_limit_orgs(orgs_to_include)
        else:
            orgs_to_include = None



        ########################
        ## Joining the data   ##
        ########################

        main_data = data['folders']
        main_data = handle_duplicates(main_data, 'Organization ID')
        # print('main_data', main_data.head(20))
        print(f'Done with folders')

        # Remove any rows where Organization ID is not in orgs_to_include
        if orgs_to_include is not None:
            main_data = main_data[main_data['Organization ID'].isin(orgs_to_include)]
            print(f'Done filtering for orgs_to_include')

        # Warn of any duplicate columns, other than Organization ID, between main_data and locations_data
        for column in main_data.columns:
            if column in data['locations'].columns and column != 'Organization ID':
                print(f'Warning: column {column} is present in both main_data and locations_data')

        locations_data = handle_duplicates(data['locations'], 'Organization ID')
        main_data = join_data(main_data, locations_data, 'Organization ID', suffix='_locations')
        # print('main_data', main_data.head(20))
        print(f'Done with locations')

        # Warn of any duplicate columns other, than Organization ID, between main_data and members_data
        for column in main_data.columns:
            if column in data['members'].columns and column != 'Organization ID':
                print(f'Warning: column {column} is present in both main_data and members_data')

        members_data = handle_duplicates(data['members'], 'Organization ID', unique_only=False)
        main_data = join_data(main_data, members_data, 'Organization ID', suffix='_members')
        # print('main_data', main_data.head(20))
        print(f'Done with members')

        # Warn of any duplicate columns other, than Organization ID, between main_data and related_collections_data
        for column in main_data.columns:
            if column in data['related_collections'].columns and column != 'Organization ID':
                print(f'Warning: column {column} is present in both main_data and related_collections_data')

        related_collections_data = handle_duplicates(data['related_collections'], 'Organization ID', unique_only=False)
        main_data = join_data(main_data, related_collections_data, 'Organization ID', suffix='_related_collections')
        # print('main_data', main_data.head(20))
        print(f'Done with related_collections')

        # Warn of any duplicate columns other, than Organization ID, between main_data and sources_data
        for column in main_data.columns:
            if column in data['sources'].columns and column != 'Organization ID':
                print(f'Warning: column {column} is present in both main_data and sources_data')

        sources_data = handle_duplicates(data['sources'], 'Organization ID', unique_only=False)
        main_data = join_data(main_data, sources_data, 'Organization ID', suffix='_sources')
        # print('main_data', main_data.head(20))
        print(f'Done with sources')

        # Warn of any duplicate columns other, than Organization ID, between main_data and subjects_data
        for column in main_data.columns:
            if column in data['subjects'].columns and column != 'Organization ID':
                print(f'Warning: column {column} is present in both main_data and subjects_data')

        subjects_data = handle_duplicates(data['subjects'], 'Organization ID', unique_only=False)
        main_data = join_data(main_data, subjects_data, 'Organization ID', suffix='_subjects')
        # print('main_data', main_data.head(20))
        print(f'Done with subjects')

        # Warn of any duplicate columns other, than Organization ID, between main_data and alternative_name_data
        for column in main_data.columns:
            if column in data['alternative_name'].columns and column != 'Organization ID':
                print(f'Warning: column {column} is present in both main_data and alternative_name_data')

        alternative_name_data = handle_duplicates(data['alternative_name'], 'Organization ID', unique_only=False)
        main_data = join_data(main_data, alternative_name_data, 'Organization ID', suffix='_alternative_name')
        # print('main_data', main_data.head(20))
        print(f'Done with alternative_name')


        save_data_to_csv(main_data, output_file)

    except Exception as e:
        print(f'An error occurred: {str(e)}')
        exit(1)
