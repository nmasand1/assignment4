import pandas as pd

def compare_csvs(file1_path, file2_path, output_path):
    # Read the CSV files
    df1 = pd.read_csv(file1_path, delimiter='.', skipinitialspace=True)
    df2 = pd.read_csv(file2_path, delimiter='.', skipinitialspace=True)

    # Clean column names to remove leading/trailing spaces and make them lowercase
    df1.columns = df1.columns.str.strip().str.lower()
    df2.columns = df2.columns.str.strip().str.lower()

    # Print cleaned column names for debugging
    print("First CSV Columns:", df1.columns.tolist())
    print("Second CSV Columns:", df2.columns.tolist())

    # Check for specific expected columns
    expected_columns_df1 = ['businessdate', 'tablename', 'rowsextracted']
    expected_columns_df2 = ['businessdate', 'tablename', 'upstreamcount', 'processedcount', 'recordtype']

    # Check if expected columns exist
    for col in expected_columns_df1:
        if col not in df1.columns:
            print(f"Missing column in first CSV: {col}")
    
    for col in expected_columns_df2:
        if col not in df2.columns:
            print(f"Missing column in second CSV: {col}")

    # Merge dataframes on BusinessDate and TableName (with case sensitivity and spaces handled)
    merged_df = pd.merge(df1, df2, on=['businessdate', 'tablename'], how='outer')

    # Initialize a list for output
    output_data = []

    # Loop through merged data
    for index, row in merged_df.iterrows():
        date = row.get('businessdate', None)
        table_name = row.get('tablename', None)
        rows_extracted = row.get('rowsextracted', 0)
        upstream_count = row.get('upstreamcount', 0)
        record_type = row.get('recordtype', 'N/A')

        # Compare RowsExtracted with UpstreamCount according to RecordType
        if record_type != 'N/A' and rows_extracted != 0:
            comparison_result = 'Match' if rows_extracted == upstream_count else 'Mismatch'
            output_data.append({
                'BusinessDate': date,
                'TableName': table_name,
                'RowsExtracted': rows_extracted,
                'UpstreamCount': upstream_count,
                'RecordType': record_type,
                'ComparisonResult': comparison_result
            })

    # Create DataFrame for output
    output_df = pd.DataFrame(output_data)

    # Write output to CSV
    output_df.to_csv(output_path, index=False)

    # Print missing dates if any
    missing_dates = merged_df[merged_df['rowsextracted'].isnull()]['businessdate'].unique()
    if missing_dates.size > 0:
        print("Missing Business Dates:")
        for date in missing_dates:
            print(date)

# Example usage
file1 = 'path_to_your_first_csv.csv'  # Replace with the actual path
file2 = 'path_to_your_second_csv.csv'  # Replace with the actual path
output_file = 'comparison_output.csv'   # Output path for comparison results

compare_csvs(file1, file2, output_file)
