import pandas as pd

# Read both CSV files
csv1 = pd.read_csv('csv1.csv')
csv2 = pd.read_csv('csv2.csv')

# Clean column names by stripping whitespace and making them lowercase
csv1.columns = csv1.columns.str.strip().str.lower()
csv2.columns = csv2.columns.str.strip().str.lower()

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
        # Get UpstreamCount for RecordType 2
        record_type_2 = group2[group2['recordtype'] == 2]
        upstream_count = record_type_2['upstreamcount'].values[0] if not record_type_2.empty else 'Missing'

        # Check the total ProcessedCount for RecordType 0 and 1
        processed_count_0 = group2[group2['recordtype'] == 0]['processedcount'].values[0] if not group2[group2['recordtype'] == 0].empty else 0
        processed_count_1 = group2[group2['recordtype'] == 1]['processedcount'].values[0] if not group2[group2['recordtype'] == 1].empty else 0
        
        # Append rows for each record type
        results.append({
            'BusinessDate': business_date,
            'RowsExtracted': rows_extracted if processed_count_0 == 0 else 'Missing',  # Keep RowsExtracted as is
            'UpstreamCount': 0,  # UpstreamCount for RecordTypes 0 and 1 is always 0
            'ProcessedCount': processed_count_0,
            'RecordType': 0,
            'ComparisonWithUpstream': 'Match' if processed_count_0 == 0 else 'Mismatch'  # Comparing processed count of 0
        })

        results.append({
            'BusinessDate': business_date,
            'RowsExtracted': rows_extracted if processed_count_1 == 0 else 'Missing',  # Keep RowsExtracted as is
            'UpstreamCount': 0,  # UpstreamCount for RecordTypes 0 and 1 is always 0
            'ProcessedCount': processed_count_1,
            'RecordType': 1,
            'ComparisonWithUpstream': 'Match' if processed_count_1 == 0 else 'Mismatch'  # Comparing processed count of 1
        })

        # Append RecordType 2
        results.append({
            'BusinessDate': business_date,
            'RowsExtracted': rows_extracted,
            'UpstreamCount': upstream_count,
            'ProcessedCount': upstream_count,  # For RecordType 2, UpstreamCount equals ProcessedCount
            'RecordType': 2,
            'ComparisonWithUpstream': 'Match' if upstream_count == rows_extracted else 'Mismatch'
        })
        
    else:
        # Handle missing data in CSV2
        results.append({
            'BusinessDate': business_date,
            'RowsExtracted': rows_extracted,
            'UpstreamCount': 'Missing',
            'ProcessedCount': 'Missing',
            'RecordType': 'Missing',
            'ComparisonWithUpstream': 'Missing'
        })

# Handle dates present in CSV2 but missing in CSV1
for business_date, group2 in csv2.groupby('businessdate'):
    if business_date not in csv1['businessdate'].values:
        # Get the total ProcessedCount for RecordType 0 and 1
        processed_count_0 = group2[group2['recordtype'] == 0]['processedcount'].sum()
        processed_count_1 = group2[group2['recordtype'] == 1]['processedcount'].sum()
        
        # UpstreamCount for RecordType 2
        upstream_count = group2[group2['recordtype'] == 2]['upstreamcount'].values[0] if not group2[group2['recordtype'] == 2].empty else 'Missing'
        
        # Append to results indicating missing data from CSV1
        results.append({
            'BusinessDate': business_date,
            'RowsExtracted': 'Missing',
            'UpstreamCount': upstream_count,
            'ProcessedCount': processed_count_0,
            'RecordType': 0,
            'ComparisonWithUpstream': 'Missing'
        })
        results.append({
            'BusinessDate': business_date,
            'RowsExtracted': 'Missing',
            'UpstreamCount': upstream_count,
            'ProcessedCount': processed_count_1,
            'RecordType': 1,
            'ComparisonWithUpstream': 'Missing'
        })

# Convert results into a DataFrame
result_df = pd.DataFrame(results)

# Perform the check for all record types
for index, row in result_df.iterrows():
    if row['RecordType'] == 2:  # For RecordType 2
        if row['RowsExtracted'] == row['UpstreamCount']:
            result_df.at[index, 'ComparisonWithUpstream'] = 'Match'
        else:
            result_df.at[index, 'ComparisonWithUpstream'] = 'Mismatch'

# Write the results to a CSV file
result_df.to_csv('comparison_results.csv', index=False)
