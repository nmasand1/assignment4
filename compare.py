import pandas as pd
import re
from datetime import datetime

def extract_date_from_filename(filename):
    # Ensure filename is a valid string, otherwise return None
    if not isinstance(filename, str):
        return None
    
    # Extracts the date (in YYYYMMDD format) from the filename
    match = re.search(r'(\d{8})', filename)  # Looks for a pattern like 20221126
    if match:
        date_str = match.group(1)
        # Return the date in YYYY-MM-DD format for easier comparison
        return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
    return None

def map_filename_to_recordtype(filename):
    # Mapping the filenames to record types based on prefixes
    if not isinstance(filename, str):
        return None
    if "Cash_TRADE_OnPrem" in filename:
        return 1  # On-Prem Data
    elif "Cash_TRADE_Original" in filename:
        return 2  # Original Data
    elif "Cash_TRADE_" in filename:
        return 0  # Cloud Data
    else:
        return None  # Unknown type, not relevant for processing

def normalize_date(date_str):
    """Normalize the date to YYYY-MM-DD format regardless of the input format."""
    if not isinstance(date_str, str):
        return None
    try:
        # Attempt parsing in MM/DD/YYYY format
        return datetime.strptime(date_str.strip(), "%m/%d/%Y").strftime("%Y-%m-%d")
    except ValueError:
        try:
            # Attempt parsing in YYYY-MM-DD format
            return datetime.strptime(date_str.strip(), "%Y-%m-%d").strftime("%Y-%m-%d")
        except ValueError:
            return None

def validate_csv_data(csv1_path, csv2_path, output_csv_path):
    # Read the CSV files
    try:
        df1 = pd.read_csv(csv1_path)
        df2 = pd.read_csv(csv2_path)
    except Exception as e:
        print(f"Error reading the CSV files: {e}")
        return

    # Clean column names: make lowercase, strip any extra spaces
    df1.columns = df1.columns.str.strip().str.lower()
    df2.columns = df2.columns.str.strip().str.lower()

    # Define the required columns for both CSVs
    required_columns_df1 = ['businessdate', 'rowsextracted', 'filename']
    required_columns_df2 = ['tablename', 'businessdate', 'upstreamcount', 'processedcount', 'recordtype']

    # Check for missing columns in both CSVs
    for col in required_columns_df1:
        if col not in df1.columns:
            print(f"Missing column in first CSV: {col}")
            return
    for col in required_columns_df2:
        if col not in df2.columns:
            print(f"Missing column in second CSV: {col}")
            return

    # Initialize a list to store validation results
    results = []

    # Normalize 'businessdate' columns in both dataframes
    df1['businessdate'] = df1['businessdate'].apply(normalize_date)
    df2['businessdate'] = df2['businessdate'].apply(normalize_date)

    # Iterate through each row in the first CSV
    for _, row in df1.iterrows():
        filename = row['filename']
        rows_extracted = row['rowsextracted']
        
        # Extract the business date from the filename
        file_business_date = extract_date_from_filename(filename)
        if file_business_date is None:
            # Log if the filename doesn't have a valid date
            results.append({
                'Filename': filename,
                'RowsExtracted': rows_extracted,
                'UpstreamCount': 'N/A',
                'ProcessedCount': 'N/A',
                'RecordType': 'N/A',
                'BusinessDate': 'N/A',
                'ValidUpstream': 'N/A',
                'ValidRowsExtracted': f"Invalid filename format: {filename}"
            })
            continue

        # Map the filename to the corresponding record type
        mapped_recordtype = map_filename_to_recordtype(filename)
        if mapped_recordtype is None:
            # Log if the filename doesn't match the known types
            results.append({
                'Filename': filename,
                'RowsExtracted': rows_extracted,
                'UpstreamCount': 'N/A',
                'ProcessedCount': 'N/A',
                'RecordType': 'N/A',
                'BusinessDate': file_business_date,
                'ValidUpstream': 'N/A',
                'ValidRowsExtracted': f"Unknown file type for {filename}"
            })
            continue

        # Filter df2 for matching businessdate and recordtype
        df2_filtered = df2[(df2['businessdate'] == file_business_date)]

        if df2_filtered.empty:
            # No matching data, log this in results
            results.append({
                'BusinessDate': file_business_date,
                'RowsExtracted': rows_extracted,
                'UpstreamCount': 'N/A',
                'ProcessedCount': 'N/A',
                'RecordType': mapped_recordtype,
                'Filename': filename,
                'ValidUpstream': 'N/A',
                'ValidRowsExtracted': f"No matching data in second CSV for BusinessDate: {file_business_date}"
            })
            print(f"No matching data in second CSV for BusinessDate: {file_business_date}")
            continue

        # Validate each record type 0, 1, 2 individually for this business date
        for recordtype in [0, 1, 2]:
            df2_type_filtered = df2_filtered[df2_filtered['recordtype'] == recordtype]

            if df2_type_filtered.empty:
                results.append({
                    'BusinessDate': file_business_date,
                    'RowsExtracted': rows_extracted,
                    'UpstreamCount': 'N/A',
                    'ProcessedCount': 'N/A',
                    'RecordType': recordtype,
                    'Filename': filename,
                    'ValidUpstream': 'N/A',
                    'ValidRowsExtracted': f"No matching data for RecordType: {recordtype}"
                })
                continue

            total_upstream_count = df2_type_filtered['upstreamcount'].sum()

            # Check if the total UpstreamCount for the recordtype matches RowsExtracted
            is_valid_rows_extracted = (total_upstream_count == rows_extracted)

            for _, df2_row in df2_type_filtered.iterrows():
                upstream_count = df2_row['upstreamcount']
                processed_count = df2_row['processedcount']

                # Compare upstream and processed counts
                is_valid_upstream = (upstream_count == processed_count)

                # Add the result row to the output
                results.append({
                    'BusinessDate': file_business_date,
                    'RowsExtracted': rows_extracted,
                    'UpstreamCount': upstream_count,
                    'ProcessedCount': processed_count,
                    'RecordType': recordtype,
                    'Filename': filename,
                    'ValidUpstream': is_valid_upstream,
                    'ValidRowsExtracted': is_valid_rows_extracted
                })

    # Create a DataFrame from the results
    results_df = pd.DataFrame(results)

    # Save results to CSV
    results_df.to_csv(output_csv_path, index=False)

    # Print results for confirmation
    print(f"Validation results saved to {output_csv_path}")

# Specify the paths to your CSV files and output CSV file
csv1_path = 'path_to_first_csv.csv'  # Update with the actual path
csv2_path = 'path_to_second_csv.csv'  # Update with the actual path
output_csv_path = 'validation_results.csv'  # Specify output path

# Run the validation
validate_csv_data(csv1_path, csv2_path, output_csv_path)
