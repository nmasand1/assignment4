import pandas as pd

def validate_csv_data(csv1_path, csv2_path, output_csv_path):
    # Load the first CSV with tab as delimiter
    df1 = pd.read_csv(csv1_path, delimiter='\t')

    # Debug: Print the columns in the first CSV
    print("Columns in first CSV:", df1.columns.tolist())

    # Load the second CSV with tab as delimiter
    df2 = pd.read_csv(csv2_path, delimiter='\t')

    # Debug: Print the columns in the second CSV
    print("Columns in second CSV:", df2.columns.tolist())

    # Ensure the necessary columns exist in both DataFrames
    required_columns_df1 = ['businessdate', 'rowsextracted', 'filename']
    required_columns_df2 = ['tablename', 'businessdate', 'upstreamcount', 'processedcount', 'recordtype']

    # Check for missing columns in the first CSV
    for col in required_columns_df1:
        if col not in df1.columns:
            print(f"Missing column in first CSV: {col}")
            return

    # Check for missing columns in the second CSV
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
            print(f"No matching data in second CSV for BusinessDate: {business_date}")
            continue

        # Initialize sums for UpstreamCount for record types 0, 1, and 2
        upstream_count_0 = df2_filtered[df2_filtered['recordtype'] == 0]['upstreamcount'].sum()
        upstream_count_1 = df2_filtered[df2_filtered['recordtype'] == 1]['upstreamcount'].sum()
        upstream_count_2 = df2_filtered[df2_filtered['recordtype'] == 2]['upstreamcount'].sum()

        # Total UpstreamCount for available record types
        total_upstream_count = upstream_count_0 + upstream_count_1 + upstream_count_2

        # Initialize a flag for overall validation
        is_valid_extracted = (upstream_count_0 + upstream_count_1 + upstream_count_2 == rows_extracted)

        # Validate each individual record
        for _, record in df2_filtered.iterrows():
            is_valid_upstream = (record['upstreamcount'] == record['processedcount'])

            # Append results for each record
            results.append({
                'BusinessDate': business_date,
                'RowsExtracted': rows_extracted,
                'RecordType': record['recordtype'],
                'UpstreamCount': record['upstreamcount'],
                'ProcessedCount': record['processedcount'],
                'ValidUpstreamCount': is_valid_upstream
            })

        # Append a summary for RowsExtracted validity
        results.append({
            'BusinessDate': business_date,
            'RowsExtracted': rows_extracted,
            'TotalUpstreamCount': total_upstream_count,
            'ValidRowsExtracted': is_valid_extracted,
            'Summary': 'Rows Extracted Check'
        })

    # Create a DataFrame from the results
    results_df = pd.DataFrame(results)

    # Save results to CSV
    results_df.to_csv(output_csv_path, index=False)

    # Print results for confirmation
    print(f"Validation results saved to {output_csv_path}")

# Specify the paths to your CSV files and output CSV file
csv1_path = 'file1.csv'  # Update with the actual path
csv2_path = 'file2.csv'  # Update with the actual path
output_csv_path = 'validation_results.csv'  # Specify output path

# Run the validation
validate_csv_data(csv1_path, csv2_path, output_csv_path)
