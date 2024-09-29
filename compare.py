import pandas as pd

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

    # Iterate through each unique BusinessDate in df1
    for _, row in df1.iterrows():
        business_date = row['businessdate']
        rows_extracted = row['rowsextracted']

        # Filter df2 for the matching BusinessDate
        df2_filtered = df2[df2['businessdate'] == business_date]

        if df2_filtered.empty:
            # No matching data, log this in results
            results.append({
                'BusinessDate': business_date,
                'RowsExtracted': rows_extracted,
                'UpstreamCount': 'N/A',
                'ProcessedCount': 'N/A',
                'RecordType': 'N/A',
                'ValidUpstream': 'N/A',
                'ValidRowsExtracted': f"No matching data in second CSV for BusinessDate: {business_date}"
            })
            print(f"No matching data in second CSV for BusinessDate: {business_date}")
            continue

        # Sum UpstreamCount for each record type 0, 1, and 2
        total_upstream_count = df2_filtered[df2_filtered['recordtype'].isin([0, 1, 2])]['upstreamcount'].sum()

        # Compare the sum of UpstreamCount (for record types 0, 1, 2) with RowsExtracted
        is_valid_rows_extracted = (total_upstream_count == rows_extracted)

        # For each row in df2_filtered, include recordtype-specific data in the output
        for _, df2_row in df2_filtered.iterrows():
            recordtype = df2_row['recordtype']
            upstream_count = df2_row['upstreamcount']
            processed_count = df2_row['processedcount']

            # Compare the upstream count for each record type with the corresponding processed count
            is_valid_upstream = (upstream_count == processed_count)

            # Add the result row to the output
            results.append({
                'BusinessDate': business_date,
                'RowsExtracted': rows_extracted,
                'UpstreamCount': upstream_count,
                'ProcessedCount': processed_count,
                'RecordType': recordtype,
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
csv1_path = 'file1_name.csv'  # Update with the actual path
csv2_path = 'file2_name.csv'  # Update with the actual path
output_csv_path = 'validation_results.csv'  # Specify output path

# Run the validation
validate_csv_data(csv1_path, csv2_path, output_csv_path)
