import pandas as pd

def load_and_filter_csv(file_path, uti_col):
    """
    Load CSV and filter out rows where UTI has 'NOAP' value.
    """
    df = pd.read_csv(file_path)
    # Filter out rows where UTI is 'NOAP'
    filtered_df = df[df[uti_col] != 'NOAP']
    return filtered_df

def compare_data(tsr_df, msr_df, uti_col, portfolio_code_col):
    """
    Compare two DataFrames based on UTI and variationportfoliocode columns.
    """
    # Merge DataFrames on 'uti' and 'variationportfoliocode'
    comparison_df = pd.merge(tsr_df, msr_df, on=[uti_col, portfolio_code_col], how='outer', indicator=True)
    
    # Calculate statistics
    matching_rows = len(comparison_df[comparison_df['_merge'] == 'both'])
    only_in_tsr = len(comparison_df[comparison_df['_merge'] == 'left_only'])
    only_in_msr = len(comparison_df[comparison_df['_merge'] == 'right_only'])
    
    # Calculate total rows for TSR and MSR
    total_tsr_rows = len(tsr_df)
    total_msr_rows = len(msr_df)
    total_combined_rows = len(comparison_df)

    # Generate statistics
    stats = {
        "Total TSR Rows": total_tsr_rows,
        "Total MSR Rows": total_msr_rows,
        "Matching Rows": matching_rows,
        "Only in TSR": only_in_tsr,
        "Only in MSR": only_in_msr,
        "Percentage Matching": (matching_rows / total_combined_rows) * 100,
        "Percentage Non-Matching (TSR or MSR)": ((only_in_tsr + only_in_msr) / total_combined_rows) * 100
    }
    
    return comparison_df, stats

def main():
    # File paths
    tsr_file = 'path/to/tsr.csv'
    msr_file = 'path/to/msr.csv'
    
    # Column names
    uti_col = 'uti'
    portfolio_code_col = 'variationportfoliocode'
    
    # Load and filter TSR file (ignoring NOAP values)
    tsr_df = load_and_filter_csv(tsr_file, uti_col)
    
    # Load MSR file
    msr_df = pd.read_csv(msr_file)
    
    # Compare TSR and MSR data
    comparison_df, stats = compare_data(tsr_df, msr_df, uti_col, portfolio_code_col)
    
    # Output statistics
    print("Comparison Statistics:")
    for key, value in stats.items():
        print(f"{key}: {value}")
    
    # Optionally, save the comparison DataFrame to a CSV file
    comparison_df.to_csv('comparison_result.csv', index=False)

if __name__ == "__main__":
    main()
