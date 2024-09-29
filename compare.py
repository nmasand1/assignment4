import pandas as pd

def validate_csv_data(csv1_path, csv2_path, output_csv_path):
    # Read the CSV files
    try:
        df1 = pd.read_csv(csv1_path)
        df2 = pd.read_csv(csv2_path)
    except Exception as e:
        print(f"Error reading the CSV files: {e}")
        return

    # Clean column names: lowercase and strip spaces for both CSVs
    df1.columns = df1.columns.str.strip().str.lower()
    df2.columns = df2.columns.str.strip().str.lower()

    # Print first few rows for inspection
    print("First few rows of first CSV (df1):")
    print(df1.head())

    print("First few rows of second CSV (df2):")
    print(df2.head())

    # Check required columns
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

    # Initialize a list to store validation results
    results = []

    # Iterate through each unique BusinessDate in df1
    for _, row in df1.iterrows():
        business_date = row['businessdate']
        rows_extracted = row['rowsextracted']

        # Filter df2 for the matching BusinessDate
        df2_filtered = df2[df2['businessdate'] == business_date]

        if df2_filtered.empty:
            # Log this as a result if no matching data is found
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

        # Process each recordtype (0, 1, 2) separately and create rows for each
        for record_type in [0, 1, 2]:
            recordtype_filtered = df2_filtered[df2_filtered['recordtype'] == record_type]

            if not recordtype_filtered.empty:
                upstream_count = recordtype_filtered['upstreamcount'].sum()
                processed_count = recordtype_filtered['processedcount'].sum()

                # Check if upstream count equals processed count
                is_valid_upstream = (upstream_count == processed_count)

                # Check if upstream count matches RowsExtracted
                is_valid_rows_extracted = (upstream_count == rows_extracted)

                # Append the result for this specific recordtype
                results.append({
                    'BusinessDate': business_date,
                    'RowsExtracted': rows_extracted,
                    'UpstreamCount': upstream_count,
                    'ProcessedCount': processed_count,
                    'RecordType': record_type,
                    'ValidUpstream': is_valid_upstream,
                    'ValidRowsExtracted': is_valid_rows_extracted
                })

            else:
                # If the recordtype is not present, add a row with N/A
                results.append({
                    'BusinessDate': business_date,
                    'RowsExtracted': rows_extracted,
                    'UpstreamCount': 'N/A',
                    'ProcessedCount': 'N/A',
                    'RecordType': record_type,
                    'ValidUpstream': 'N/A',
                    'ValidRowsExtracted': 'N/A'
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
