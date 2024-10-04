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

# Group data by BusinessDate in both CSVs
grouped_csv1 = csv1.groupby('businessdate')
grouped_csv2 = csv2.groupby('businessdate')

# Loop through each group in CSV1
for business_date, group1 in grouped_csv1:
    # Get the total RowsExtracted for the current business date
    total_rows_extracted = group1['rowsextracted'].sum()
    
    # Get the corresponding group from CSV2
    group2 = grouped_csv2.get_group(business_date) if business_date in grouped_csv2.groups else None

    if group2 is not None:
        # Sum ProcessedCount for RecordType 0 and 1 in CSV2
        processed_sum = group2[group2['recordtype'].isin([0, 1])]['processedcount'].sum()
        
        # Get the upstream count for RecordType 2
        upstream_count = group2[group2['recordtype'] == 2]['upstreamcount'].values[0] if not group2[group2['recordtype'] == 2].empty else None
        
        # Compare UpstreamCount with RowsExtracted
        match_status = 'Match' if upstream_count == total_rows_extracted else 'Mismatch'

        # Append to results
        results.append({
            'BusinessDate': business_date,
            'TotalRowsExtracted': total_rows_extracted,
            'UpstreamCount': upstream_count,
            'ProcessedCount (0+1)': processed_sum,
            'MatchStatus': match_status
        })
    else:
        # Handle missing data in CSV2
        results.append({
            'BusinessDate': business_date,
            'TotalRowsExtracted': total_rows_extracted,
            'UpstreamCount': 'Missing',
            'ProcessedCount (0+1)': 'Missing',
            'MatchStatus': 'Missing'
        })

# Handle dates present in CSV2 but missing in CSV1
for business_date, group2 in grouped_csv2:
    if business_date not in grouped_csv1.groups:
        # Get the total ProcessedCount for RecordType 0 and 1
        processed_sum = group2[group2['recordtype'].isin([0, 1])]['processedcount'].sum()
        upstream_count = group2[group2['recordtype'] == 2]['upstreamcount'].values[0] if not group2[group2['recordtype'] == 2].empty else None
        
        # Append to results indicating missing data from CSV1
        results.append({
            'BusinessDate': business_date,
            'TotalRowsExtracted': 'Missing',
            'UpstreamCount': upstream_count,
            'ProcessedCount (0+1)': processed_sum,
            'MatchStatus': 'Missing'
        })

# Convert results into a DataFrame
result_df = pd.DataFrame(results)

# Write the results to a CSV file
result_df.to_csv('comparison_results.csv', index=False)
