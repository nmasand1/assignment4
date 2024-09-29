import pandas as pd
import re

def extract_date_from_filename(filename):
    # Check if the filename is a valid string
    if pd.notna(filename) and isinstance(filename, str):
        # Extract the date (in YYYYMMDD format) from the filename
        match = re.search(r'(\d{8})', filename)  # Looks for a pattern like 20221126
        if match:
            date_str = match.group(1)
            # Return the date in YYYY-MM-DD format for easier comparison
            return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
    return None

def map_filename_to_recordtype(filename):
    if "Cash_TRADE_OnPrem" in filename:
        return 1  # On-Prem Data
    elif "Cash_TRADE_Original" in filename:
        return 2  # Original Data
    elif "Cash_TRADE_" in filename:
        return 0  # Cloud Data
    else:
        return None

def validate_csv_data(csv1_path, csv2_path, output_csv_path):
    try:
        df1 = pd.read_csv(csv1_path)
        df2 = pd.read_csv(csv2_path)
    except Exception as e:
        print(f"Error reading the CSV files: {e}")
        return

    df1.columns = df1.columns.str.strip().str.lower()
    df2.columns = df2.columns.str.strip().str.lower()

    required_columns_df1 = ['businessdate', 'rowsextracted', 'filename']
    required_columns_df2 = ['tablename', 'businessdate', 'upstreamcount', 'processedcount', 'recordtype']

    for col in required_columns_df1:
        if col not in df1.columns:
            print(f"Missing column in first CSV: {col}")
            return
    for col in required_columns_df2:
        if col not in df2.columns:
            print(f"Missing column in second CSV: {col}")
            return

    results = []

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
        df2_filtered = df2[(df2['businessdate'] == file_business_date) & (df2['recordtype'] == mapped_recordtype)]

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
                'ValidRowsExtracted': f"No matching data in second CSV for BusinessDate: {file_business_date}, RecordType: {mapped_recordtype}"
            })
            print(f"No matching data in second CSV for BusinessDate: {file_business_date}, RecordType: {mapped_recordtype}")
            continue

        total_upstream_count = df2_filtered['upstreamcount'].sum()
        is_valid_rows_extracted = (total_upstream_count == rows_extracted)

        for _, df2_row in df2_filtered.iterrows():
            recordtype = df2_row['recordtype']
            upstream_count = df2_row['upstreamcount']
            processed_count = df2_row['processedcount']

            is_valid_upstream = (upstream_count == processed_count)

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

    results_df = pd.DataFrame(results)
    results_df.to_csv(output_csv_path, index=False)

    print(f"Validation results saved to {output_csv_path}")

csv1_path = 'file1_name.csv'
csv2_path = 'file2_name.csv'
output_csv_path = 'validation_results2.csv'

validate_csv_data(csv1_path, csv2_path, output_csv_path)
