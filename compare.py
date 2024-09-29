import pandas as pd

def validate_csv_data(csv1_path, csv2_path, output_csv_path):
    # Read the CSV files
    try:
        df1 = pd.read_csv(csv1_path)
        df2 = pd.read_csv(csv2_path)
    except Exception as e:
        print(f"Error reading the CSV files: {e}")
        return

    # Clean column names: make lowercase, strip any extra spaces, and remove case sensitivity
    df1.columns = df1.columns.str.strip().str.lower()
    df2.columns = df2.columns.str.strip().str.lower()

    # Define the required columns for both CSVs
    required_columns_df1 = ['businessdate', 'rowsextracted', 'filename']
    required_columns_df2 = ['tablename', 'businessdate', 'upstreamcount', 'processedcount', 'recordtype']

    # Check if all required columns are present in both CSVs
    missing_cols_df1 = [col for col in required_columns_df1 if col not in df1.columns]
    missing_cols_df2 = [col for col in required_columns_df2 if col not in df2.columns]

    if missing_cols_df1:
        print(f"Missing columns in first CSV: {missing_cols_df1}")
        return
    if missing_cols_df2:
        print(f"Missing columns in second CSV: {missing_cols_df2}")
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
            # If no matching data, log this as a result
            results.append({
                'BusinessDate': business_date,
                'RowsExtracted': rows_extracted,
                'RecordType': 'N/A',
                'UpstreamCount': 'N/A',
                'ProcessedCount': 'N/A',
                'ValidUpstream': 'N/A',
                'ValidRowsExtracted': f"No matching data in second CSV for BusinessDate: {business_date}"
            })
            print(f"No matching data in second CSV for BusinessDate: {business_date}")
            continue

        # Process each record type (0, 1, 2) separately
        for recordtype in [0, 1, 2]:
            # Filter the rows based on the recordtype
            df2_recordtype = df2_filtered[df2_filtered['recordtype'] == recordtype]

            if df2_recordtype.empty:
                # Skip if no data for this recordtype
                continue

            # Calculate the sum of UpstreamCount and get the unique ProcessedCount
            upstream_count_sum = df2_recordtype['upstreamcount'].sum()
            processed_count_sum = df2_recordtype['processedcount'].unique()

            # Check if the UpstreamCount equals ProcessedCount for this recordtype
            is_valid_upstream = upstream_count_sum in processed_count_sum

            # Check if the sum of UpstreamCount matches RowsExtracted
            is_valid_rows_extracted = (upstream_count_sum == rows_extracted)

            # Append the result to the list
            results.append({
                'BusinessDate': business_date,
                'RowsExtracted': rows_extracted,
                'RecordType': recordtype,
                'UpstreamCount': upstream_count_sum,
                'ProcessedCount': ', '.join(map(str, processed_count_sum)),  # Combine multiple processed counts
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
