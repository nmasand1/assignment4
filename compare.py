import pandas as pd

# Read both CSV files
csv1 = pd.read_csv('csv1.csv')
csv2 = pd.read_csv('csv2.csv')

# Clean column names by stripping whitespace and making them lowercase (or adjust case based on your CSV)
csv1.columns = csv1.columns.str.strip().str.lower()
csv2.columns = csv2.columns.str.strip().str.lower()

# Print the columns to check their names
print("CSV1 Columns:", csv1.columns)
print("CSV2 Columns:", csv2.columns)

# Normalize TableName and BusinessDate columns to uppercase and datetime for consistency
csv1['tablename'] = csv1['tablename'].str.strip().str.upper()
csv2['tablename'] = csv2['tablename'].str.strip().str.upper()

csv1['businessdate'] = pd.to_datetime(csv1['businessdate'])
csv2['businessdate'] = pd.to_datetime(csv2['businessdate'])

# Create a DataFrame to store the results
results = []

# Group data by BusinessDate and TableName in both CSVs
grouped_csv1 = csv1.groupby(['businessdate', 'tablename'])
grouped_csv2 = csv2.groupby(['businessdate', 'tablename'])

# Loop through each group in CSV1
for (business_date, table_name), group1 in grouped_csv1:
    # Get the corresponding group from CSV2
    group2 = grouped_csv2.get_group((business_date, table_name)) if (business_date, table_name) in grouped_csv2.groups else None

    if group2 is not None:
        # Sum ProcessedCount for RecordType 0 and 1 in CSV2
        processed_sum = group2[group2['recordtype'].isin([0, 1])]['processedcount'].sum()
        
        # Get the upstream count for RecordType 2
        upstream_count = group2[group2['recordtype'] == 2]['upstreamcount'].values[0]
        
        # Compare with RowsExtracted from CSV1
        for _, row1 in group1.iterrows():
            rowsextracted = row1['rowsextracted']
            filename = row1['filename']

            # Check if upstream_count equals RowsExtracted
            match_status = 'Match' if upstream_count == rowsextracted else 'Mismatch'

            # Append to results
            results.append({
                'BusinessDate': business_date,
                'TableName': table_name,
                'FileName': filename,
                'UpstreamCount': upstream_count,
                'RowsExtracted': rowsextracted,
                'ProcessedCount (0+1)': processed_sum,
                'ProcessedCount (2)': upstream_count,
                'MatchStatus': match_status
            })
    else:
        # Handle missing data in CSV2
        for _, row1 in group1.iterrows():
            results.append({
                'BusinessDate': business_date,
                'TableName': table_name,
                'FileName': row1['filename'],
                'UpstreamCount': 'Missing',
                'RowsExtracted': row1['rowsextracted'],
                'ProcessedCount (0+1)': 'Missing',
                'ProcessedCount (2)': 'Missing',
                'MatchStatus': 'Missing'
            })

# Convert results into a DataFrame
result_df = pd.DataFrame(results)

# Write the results to a CSV file
result_df.to_csv('comparison_results.csv', index=False)
