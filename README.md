# Purpose

Combines 7 FileMakerPro export merge-CSV format tables into one, for easier subsequent processing.

## Installation

1. Clone the repository
2. Create a virtual environment and activate it (optional but recommended)
```bash
python3 -m venv ./env
source ./env/bin/activate
```
3. Install the required packages
```bash
pip install -r requirements.txt
```

## Usage

This script denormalizes data from the FileMaker Pro database export into a single CSV file. It can process input either from a directory containing the required CSV files or from a zip file containing these files. The output is a denormalized CSV file that combines information from all input files.

The directory or zip file must contain the following 7 CSV files:
- alternative_name.csv
- folders.csv
- locations.csv
- members.csv
- related_collections.csv
- sources.csv
- subjects.csv

### Note
Warnings such as
```
Warning: column Notes is present in both main_data and sources_data
```
are expected and can be ignored. They are generated when the script encounters columns with the same name in multiple input files. The script will rename these columns to avoid conflicts.


### Command Line Arguments

- `--input_dir`: Specifies the path to a directory containing all 7 required FMP CSV files. You must specify either `input_dir` or `input_zip`, but not both.
- `--input_zip`: Specifies the path to the zip file containing all 7 required FMP CSV files. You must specify either `input_dir` or `input_zip`, but not both.
- `--output_path`: Specifies the path for the output CSV file or the directory where the denormalized data will be saved. If a directory is specified, the output file will be named `fmp_denormalized.csv` within that directory. If a file is specified, the output will be saved to that file. In either case, any existing file with the same name will be overwritten.
- `--limit_orgs`: (Optional) Limits the organizations to include in the output. Specify a path to a text or CSV file containing a list of organization IDs to include.

### Examples

1. **Using a Directory of CSV Files for input and a directory for output**

   ```bash
   python fmp_denormalize.py --input_dir /path/to/csv/files --output_path /path/to/output/

2. **Using a Zip File of CSV Files for input and a file for output**

   ```bash
   python fmp_denormalize.py --input_zip /path/to/csv/files.zip --output_path /path/to/output/combined_files_2024-07-16.csv
   ```

3. **Limiting Organizations**

   ```bash
    python fmp_denormalize.py --input_dir /path/to/csv/files --output_path /path/to/output/fmp_denormalized.csv --limit_orgs /path/to/orgs.txt
    ```

---
