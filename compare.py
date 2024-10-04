import pandas as pd

def compare_csvs(file1_path, file2_path, output_path):
    # Read the CSV files
    df1 = pd.read_csv(file1_path)
    df2 = pd.read_csv(file2_path)

    # Ensure the column names are stripped of any leading/trailing whitespace
    df1.columns = df1.columns.str.strip()
    df2.columns = df2.columns.str.strip()

    # Create a list to hold the results
    results = []

    # Extract unique BusinessDates from both dataframes
    unique_dates_df1 = df1['BusinessDate'].unique()
    unique_dates_df2 = df2['BusinessDate'].unique()

    # Find missing dates
    missing_dates = set(unique_dates_df1) - set(unique_dates_df2)
    
    if missing_dates:
        print(f"Missing Dates: {missing_dates}")
    
    # Perform the comparison
    for index, row in df1.iterrows():
        business_date = row['BusinessDate']
        rows_extracted = row['RowsExtracted']
        table_name = row['TableName']

        # Filter the second dataframe based on the same BusinessDate and TableName
        matching_rows = df2[(df2['BusinessDate'] == business_date) & (df2['TableName'].str.strip() == table_name)]
        
        for _, match_row in matching_rows.iterrows():
            record_type = match_row['RecordType']
            upstream_count = match_row['UpstreamCount']
            
            # Prepare the output row
            output_row = {
                'BusinessDate': business_date,
                'TableName': table_name,
                'RowsExtracted': rows_extracted,
                'UpstreamCount': upstream_count,
                'RecordType': record_type,
                'Match': 'Match' if rows_extracted == upstream_count else 'Mismatch'
            }
            results.append(output_row)

    # Create a DataFrame for results
    results_df = pd.DataFrame(results)

    # Write results to output CSV
    results_df.to_csv(output_path, index=False)
    print(f"Output saved to {output_path}")

# Usage
file1_path = 'csv1.csv'  # Update with your first CSV file path
file2_path = 'csv2.csv'  # Update with your second CSV file path
output_path = 'output.csv'      # Update with desired output file path

compare_csvs(file1_path, file2_path, output_path)
