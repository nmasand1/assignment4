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

        # Calculate total UpstreamCount for recordtypes 0 and 2
        upstream_count_0 = df2_filtered[df2_filtered['recordtype'] == 0]['upstreamcount'].sum()
        upstream_count_2 = df2_filtered[df2_filtered['recordtype'] == 2]['upstreamcount'].sum()
        
        # Calculate the sum of upstream counts for 0 and 2
        total_upstream_count = upstream_count_0 + upstream_count_2

        # Calculate total ProcessedCount for recordtypes 0, 1, and 2
        processed_count = df2_filtered[df2_filtered['recordtype'].isin([0, 1, 2])]['processedcount'].sum()

        # Validate and append results
        is_valid_upstream = (total_upstream_count == processed_count)
        is_valid_extracted = (total_upstream_count == rows_extracted)

        results.append({
            'BusinessDate': business_date,
            'RowsExtracted': rows_extracted,
            'TotalUpstreamCount': total_upstream_count,
            'ProcessedCount': processed_count,
            'ValidUpstreamCount': is_valid_upstream,
            'ValidRowsExtracted': is_valid_extracted
        })

    # Create a DataFrame from the results
    results_df = pd.DataFrame(results)

    # Save results to CSV
    results_df.to_csv(output_csv_path, index=False)

    # Print results for confirmation
    print(f"Validation results saved to {output_csv_path}")

# Specify the paths to your CSV files and output CSV file
csv1_path = 'path_to_first_csv.tsv'  # Update with the actual path
csv2_path = 'path_to_second_csv.tsv'  # Update with the actual path
output_csv_path = 'validation_results.csv'  # Specify output path

# Run the validation
validate_csv_data(csv1_path, csv2_path, output_csv_path)
