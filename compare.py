import pandas as pd

def get_record_type(filename):
    # Determine RecordType based on the filename
    if 'OnPrem' in filename:
        return 1  # Map to RecordType 1
    elif 'Original' in filename:
        return 2  # Map to RecordType 2
    else:
        return 0  # Default RecordType for other cases

def compare_csvs(file1_path, file2_path, output_path):
    # Read the CSV files
    df1 = pd.read_csv(file1_path)
    df2 = pd.read_csv(file2_path)

    # Strip leading/trailing whitespace from column names
    df1.columns = df1.columns.str.strip()
    df2.columns = df2.columns.str.strip()

    # Print the columns of the first DataFrame and the first few rows
    print("First CSV Columns:", df1.columns.tolist())
    print("First CSV Sample Data:\n", df1.head())
    print("Second CSV Columns:", df2.columns.tolist())
    print("Second CSV Sample Data:\n", df2.head())

    # Check if 'FileName' is among the columns
    if 'FileName' not in df1.columns:
        print("Error: 'FileName' column not found in the first CSV. Available columns are:", df1.columns.tolist())
        return

    # Convert numeric columns to appropriate types
    df1['RowsExtracted'] = pd.to_numeric(df1['RowsExtracted'], errors='coerce')

    # Create a list to hold the results
    results = []

    # Iterate through the first DataFrame
    for index, row in df1.iterrows():
        business_date = row['BusinessDate']
        rows_extracted = row['RowsExtracted']
        filename = row['FileName']  # Get the filename

        # Get the corresponding RecordType based on the filename
        record_type = get_record_type(filename)

        # Filter the second DataFrame for matching BusinessDate and RecordType
        matching_rows = df2[(df2['BusinessDate'] == business_date) & (df2['RecordType'] == record_type)]

        # Check for upstream counts
        if not matching_rows.empty:
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
            print(f"No matching upstream counts found for {business_date} with RecordType {record_type}.")

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
