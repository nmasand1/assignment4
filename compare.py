import pandas as pd

def validate_csv_data(csv1_path, csv2_path):
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

    # Initialize lists to store validation results
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

        # Calculate total UpstreamCount for recordtypes 0, 1, and 2
        upstream_counts = df2_filtered[df2_filtered['recordtype'].isin([0, 1, 2])]['upstreamcount']
        total_upstream_count = upstream_counts.sum()

        # Calculate total ProcessedCount for recordtypes 0, 1, and 2
        processed_count = df2_filtered[df2_filtered['recordtype'].isin([0, 1, 2])]['processedcount'].sum()

        # Check if total upstream count equals processed count
        if total_upstream_count == processed_count:
            results.append((business_date, 'Valid: UpstreamCount equals ProcessedCount'))
        else:
            results.append((business_date, 'Invalid: UpstreamCount does not equal ProcessedCount'))

        # Check if sum of upstream counts matches RowsExtracted
        if total_upstream_count == rows_extracted:
            results.append((business_date, 'Valid: UpstreamCount matches RowsExtracted'))
        else:
            results.append((business_date, 'Invalid: UpstreamCount does not match RowsExtracted'))

    # Print results
    for business_date, result in results:
        print(f"{business_date}: {result}")

# Specify the paths to your CSV files
csv1_path = 'path_to_first_csv.tsv'  # Update with the actual path
csv2_path = 'path_to_second_csv.tsv'  # Update with the actual path

# Run the validation
validate_csv_data(csv1_path, csv2_path)
