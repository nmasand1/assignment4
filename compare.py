import pandas as pd

def compare_csvs(file1_path, file2_path, output_path):
    # Read the CSV files
    df1 = pd.read_csv(file1_path)
    df2 = pd.read_csv(file2_path)

    # Ensure the column names are stripped of any leading/trailing whitespace
    df1.columns = df1.columns.str.strip()
    df2.columns = df2.columns.str.strip()

    # Convert numeric columns to appropriate types
    df1['RowsExtracted'] = pd.to_numeric(df1['RowsExtracted'], errors='coerce')

    # Create a mapping of filenames to RecordTypes
    filename_to_recordtype = {
        'Cash_TRADEMESSAGE_OnPrem_20230330': 1,
        'Cash_TRADEMESSAGE_20230330': 0,
        'Cash_TRADEMESSAGE_Original_20230330': 2
    }

    # Create a list to hold the results
    results = []

    # Perform the comparison based on BusinessDate and mapped RecordType
    for index, row in df1.iterrows():
        business_date = row['BusinessDate']
        rows_extracted = row['RowsExtracted']
        filename = row['FileName']  # Assuming the filename is in the column 'FileName'

        # Get the corresponding RecordType from the filename mapping
        record_type = filename_to_recordtype.get(filename)
        
        if record_type is not None:
            # Filter the second dataframe for the same BusinessDate and RecordType
            matching_rows = df2[(df2['BusinessDate'] == business_date) & (df2['RecordType'] == record_type)]
            
            # Debug print for matching rows
            print(f"Matching Rows Found for {business_date} and RecordType {record_type}: {len(matching_rows)}")

            for _, match_row in matching_rows.iterrows():
                upstream_count = match_row['UpstreamCount']
                
                # Prepare the output row
                output_row = {
                    'BusinessDate': business_date,
                    'RowsExtracted': rows_extracted,
                    'UpstreamCount': upstream_count,
                    'RecordType': record_type,
                    'Match': 'Match' if rows_extracted == upstream_count else 'Mismatch'
                }
                results.append(output_row)
        else:
            print(f"No mapping found for filename: {filename}")

    # Check if results are empty
    if not results:
        print("No matches found. Please check the input data.")
    else:
        # Create a DataFrame for results
        results_df = pd.DataFrame(results)

        # Write results to output CSV
        results_df.to_csv(output_path, index=False)
        print(f"Output saved to {output_path}")

# Usage
file1_path = 'path/to/your/first_csv.csv'  # Update with your first CSV file path
file2_path = 'path/to/your/second_csv.csv'  # Update with your second CSV file path
output_path = 'path/to/your/output.csv'      # Update with desired output file path

compare_csvs(file1_path, file2_path, output_path)
