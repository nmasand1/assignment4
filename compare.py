import pandas as pd

# Reading the first CSV with data on business dates, table names, rows extracted, and file names
csv1 = pd.read_csv('csv1.csv')

# Reading the second CSV with data on table names, business dates, upstream and processed counts, and record types
csv2 = pd.read_csv('csv2.csv')

# Clean up column names by removing extra spaces (if needed)
csv1.columns = csv1.columns.str.strip()
csv2.columns = csv2.columns.str.strip()

# Initialize a list to store comparison results
comparison_results = []

# Group by 'TableName' and 'BusinessDate' to handle each business date separately
for (table_name, business_date), group in csv2.groupby(['TableName', 'BusinessDate']):
    
    # Generate dynamic record_type_map based on the business date
    dynamic_record_type_map = {
        0: f'Cash_TRADEMESSAGE_{business_date.replace("-", "")}',  # Cash_TRADEMESSAGE_<BusinessDate>
        1: f'Cash_TRADEMESSAGE_OnPrem_{business_date.replace("-", "")}',  # Cash_TRADEMESSAGE_OnPrem_<BusinessDate>
        2: f'Cash_TRADEMESSAGE_Original_{business_date.replace("-", "")}'  # Cash_TRADEMESSAGE_Original_<BusinessDate>
    }
    
    # Filter rows by RecordType within the group
    processed_0 = group[group['RecordType'] == 0]['ProcessedCount'].sum()
    processed_1 = group[group['RecordType'] == 1]['ProcessedCount'].sum()
    processed_2 = group[group['RecordType'] == 2]['ProcessedCount'].sum()

    # Check if the sum of RecordType 0 and 1 matches RecordType 2
    match = (processed_0 + processed_1 == processed_2)
    
    # Get the UpstreamCount from RecordType 2 (it will always have a non-zero value)
    upstream_count = group[group['RecordType'] == 2]['UpstreamCount'].values[0]
    
    # Find matching row in csv1 for the current table and business date
    matching_row = csv1[(csv1['FileName'] == dynamic_record_type_map[2]) & 
                        (csv1['BusinessDate'] == business_date) & 
                        (csv1['RowsExtracted'] == upstream_count)]

    # Check if rows extracted match the upstream count
    rows_extracted_match = not matching_row.empty

    # Append results for each business date and table name
    comparison_results.append({
        'TableName': table_name,
        'BusinessDate': business_date,
        'ProcessedCount_Match': match,
        'UpstreamCount_Match': rows_extracted_match,
        'UpstreamCount (CSV2)': upstream_count,
        'RowsExtracted (CSV1)': matching_row['RowsExtracted'].values[0] if rows_extracted_match else 'N/A'
    })

# Convert the results into a DataFrame
results_df = pd.DataFrame(comparison_results)

# Export the comparison results to a CSV
results_df.to_csv('comparison_results.csv', index=False)

# Identify missing business dates between the two CSVs
csv1_dates = set(csv1['BusinessDate'])
csv2_dates = set(csv2['BusinessDate'])

missing_in_csv1 = csv2_dates - csv1_dates
missing_in_csv2 = csv1_dates - csv2_dates

# Create a DataFrame for missing dates
missing_dates_df = pd.DataFrame({
    'Missing in CSV1': list(missing_in_csv1),
    'Missing in CSV2': list(missing_in_csv2)
})

# Export missing dates to a separate CSV
missing_dates_df.to_csv('missing_dates.csv', index=False)

print("Comparison complete. Results saved to 'comparison_results.csv' and 'missing_dates.csv'.")
