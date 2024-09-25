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
        
        # Total UpstreamCount for record types 0, 1, and 2
        total_upstream_count = upstream_count_0 + upstream_count_1 + upstream_count_2

        # Check if UpstreamCount equals ProcessedCount for record types 0, 1, and 2
        valid_upstream = all(
            df2_filtered[df2_filtered['recordtype'].isin([0, 1, 2])]['upstreamcount'] == 
            df2_filtered[df2_filtered['recordtype'].isin([0, 1, 2])]['processedcount']
        )

        # Check if total UpstreamCount matches RowsExtracted
        valid_rows_extracted = (total_upstream_count == rows_extracted)

        # Append the result to the list
        results.append({
            'BusinessDate': business_date,
            'RowsExtracted': rows_extracted,
            'UpstreamCount': total_upstream_count,
            'ProcessedCount': df2_filtered['processedcount'].sum(),  # Assume sum of processed counts from all records
            'ValidUpstream': valid_upstream,
            'ValidRowsExtracted': valid_rows_extracted
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
