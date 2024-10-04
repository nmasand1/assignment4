import pandas as pd

# Read both CSV files
csv1 = pd.read_csv('csv1.csv')
csv2 = pd.read_csv('csv2.csv')

# Clean and normalize TableName and BusinessDate columns
csv1['TableName'] = csv1['TableName'].str.strip().str.upper()
csv2['TableName'] = csv2['TableName'].str.strip().str.upper()

csv1['BusinessDate'] = pd.to_datetime(csv1['BusinessDate'])
csv2['BusinessDate'] = pd.to_datetime(csv2['BusinessDate'])

# Create a DataFrame to store the results
results = []

# Group data by BusinessDate and TableName in both CSVs
grouped_csv1 = csv1.groupby(['BusinessDate', 'TableName'])
grouped_csv2 = csv2.groupby(['BusinessDate', 'TableName'])

# Loop through each group in CSV1
for (business_date, table_name), group1 in grouped_csv1:
    # Get the corresponding group from CSV2
    group2 = grouped_csv2.get_group((business_date, table_name)) if (business_date, table_name) in grouped_csv2.groups else None

    if group2 is not None:
        # Sum ProcessedCount for RecordType 0 and 1 in CSV2
        processed_sum = group2[group2['RecordType'].isin([0, 1])]['ProcessedCount'].sum()
        
        # Get the upstream count for RecordType 2
        upstream_count = group2[group2['RecordType'] == 2]['UpstreamCount'].values[0]
        
        # Compare with RowsExtracted from CSV1
        for _, row1 in group1.iterrows():
            rowsextracted = row1['RowsExtracted']
            filename = row1['FileName']

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
                'FileName': row1['FileName'],
                'UpstreamCount': 'Missing',
                'RowsExtracted': row1['RowsExtracted'],
                'ProcessedCount (0+1)': 'Missing',
                'ProcessedCount (2)': 'Missing',
                'MatchStatus': 'Missing'
            })

# Convert results into a DataFrame
result_df = pd.DataFrame(results)

# Write the results to a CSV file
result_df.to_csv('comparison_results.csv', index=False)
