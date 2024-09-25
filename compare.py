import pandas as pd

def validate_csv_data(csv1_path, csv2_path, output_csv_path):
    # Try different delimiters if necessary
    try:
        df1 = pd.read_csv(csv1_path, delimiter='\t')
    except Exception as e:
        print(f"Error reading first CSV with tab delimiter: {e}")
        print("Trying to read with other common delimiters...")
        df1 = pd.read_csv(csv1_path)  # Fall back to default comma delimiter if needed

    try:
        df2 = pd.read_csv(csv2_path, delimiter='\t')
    except Exception as e:
        print(f"Error reading second CSV with tab delimiter: {e}")
        print("Trying to read with other common delimiters...")
        df2 = pd.read_csv(csv2_path)  # Fall back to default comma delimiter if needed

    # Strip any extra spaces or invisible characters from column names and make them lowercase
    df1.columns = df1.columns.str.strip().str.lower()
    df2.columns = df2.columns.str.strip().str.lower()

    # Debugging: print the first few rows to inspect data
    print("First few rows of first CSV (df1):")
    print(df1.head())
    
    print("First few rows of second CSV (df2):")
    print(df2.head())

    # Print cleaned columns for both CSVs to verify
    print("Columns in first CSV (cleaned and lowercase):", df1.columns.tolist())
    print("Columns in second CSV (cleaned and lowercase):", df2.columns.tolist())

    # Define the required columns for both CSVs, all in lowercase
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
            # If no matching data, log this as a result
            results.append({
                'BusinessDate': business_date,
                'RowsExtracted': rows_extracted,
                'UpstreamCount': 'N/A',
                'ProcessedCount': 'N/A',
                'ValidUpstream': 'N/A',
                'ValidRowsExtracted': f"No matching data in second CSV for BusinessDate: {business_date}"
            })
            print(f"No matching data in second CSV for BusinessDate: {business_date}")
            continue

        # Initialize sums for UpstreamCount for record types 0, 1, and 2
        upstream_count_0 = df2_filtered[df2_filtered['recordtype'] == 0]['upstreamcount'].sum()
        upstream_count_1 = df2_filtered[df2_filtered['recordtype'] == 1]['upstreamcount'].sum()
        upstream_count_2 = df2_filtered[df2_filtered['recordtype'] == 2]['upstreamcount'].sum()

        # Total UpstreamCount for record types 0, 1, and 2
        total_upstream_count = upstream_count_0 + upstream_count_1 + upstream_count_2

        # Extract the unique processed counts for matching BusinessDate
        processed_count = df2_filtered['processedcount'].unique()

        # Check if the total UpstreamCount equals any of the ProcessedCounts for the record types 0, 1, and 2
        is_valid_upstream = total_upstream_count in processed_count

        # Check if total UpstreamCount matches RowsExtracted
        is_valid_rows_extracted = (total_upstream_count == rows_extracted)

        # Append the result to the list
        results.append({
            'BusinessDate': business_date,
            'RowsExtracted': rows_extracted,
            'UpstreamCount': total_upstream_count,
            'ProcessedCount': processed_count,  # Keep it as a unique value
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
csv1_path = 'path_to_first_csv.tsv'  # Update with the actual path
csv2_path = 'path_to_second_csv.tsv'  # Update with the actual path
output_csv_path = 'validation_results.csv'  # Specify output path

# Run the validation
validate_csv_data(csv1_path, csv2_path, output_csv_path)
