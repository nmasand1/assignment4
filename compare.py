import pandas as pd

# Read both CSV files
csv1 = pd.read_csv('csv1.csv')
csv2 = pd.read_csv('csv2.csv')

# Clean column names by stripping whitespace and making them lowercase
csv1.columns = csv1.columns.str.strip().str.lower()
csv2.columns = csv2.columns.str.strip().str.lower()

# Print the columns to check their names
print("CSV1 Columns:", csv1.columns)
print("CSV2 Columns:", csv2.columns)

# Normalize BusinessDate to datetime for consistency
csv1['businessdate'] = pd.to_datetime(csv1['businessdate'])
csv2['businessdate'] = pd.to_datetime(csv2['businessdate'])

# Create a DataFrame to store the results
results = []

# Loop through each row in CSV1
for _, row1 in csv1.iterrows():
    business_date = row1['businessdate']
    rows_extracted = row1['rowsextracted']
    
    # Check for matching business date in CSV2
    group2 = csv2[csv2['businessdate'] == business_date]
    
    if not group2.empty:
        # Filter for RecordType 2 to get UpstreamCount
        record_type_2 = group2[group2['recordtype'] == 2]
        
        if not record_type_2.empty:
            upstream_count = record_type_2['upstreamcount'].values[0]
            match_status = 'Match' if upstream_count == rows_extracted else 'Mismatch'
        else:
            upstream_count = 'Missing'
            match_status = 'Mismatch'
        
        # Get total processed count for RecordType 0 and 1
        processed_sum = group2[group2['recordtype'].isin([0, 1])]['processedcount'].sum()
        
        # Append to results
        results.append({
            'BusinessDate': business_date,
            'RowsExtracted': rows_extracted,
            'UpstreamCount': upstream_count,
            'ProcessedCount (0+1)': processed_sum,
            'MatchStatus': match_status
        })
    else:
        # Handle missing data in CSV2
        results.append({
            'BusinessDate': business_date,
            'RowsExtracted': rows_extracted,
            'UpstreamCount': 'Missing',
            'ProcessedCount (0+1)': 'Missing',
            'MatchStatus': 'Missing'
        })

# Handle dates present in CSV2 but missing in CSV1
for business_date, group2 in csv2.groupby('businessdate'):
    if business_date not in csv1['businessdate'].values:
        # Get the total ProcessedCount for RecordType 0 and 1
        processed_sum = group2[group2['recordtype'].isin([0, 1])]['processedcount'].sum()
        upstream_count = group2[group2['recordtype'] == 2]['upstreamcount'].values[0] if not group2[group2['recordtype'] == 2].empty else None
        
        # Append to results indicating missing data from CSV1
        results.append({
            'BusinessDate': business_date,
            'RowsExtracted': 'Missing',
            'UpstreamCount': upstream_count,
            'ProcessedCount (0+1)': processed_sum,
            'MatchStatus': 'Missing'
        })

# Convert results into a DataFrame
result_df = pd.DataFrame(results)

# Write the results to a CSV file
result_df.to_csv('comparison_results.csv', index=False)
