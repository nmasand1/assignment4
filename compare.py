import pandas as pd

def load_and_filter_tsr(file_path, uti_col):
    """
    Load TSR CSV and filter out rows where UTI has 'NOAP' value.
    """
    tsr_df = pd.read_csv(file_path)
    # Filter out rows where UTI is 'NOAP'
    filtered_tsr_df = tsr_df[tsr_df[uti_col] != 'NOAP']
    return filtered_tsr_df

def compare_variation_portfolio(tsr_df, msr_df, portfolio_code_col):
    """
    Compare variationportfoliocode from TSR and MSR.
    TSR is already filtered to include only rows with valid UTI.
    """
    # Extract the variationportfoliocode columns for comparison
    tsr_codes = tsr_df[portfolio_code_col].unique()  # Unique codes from TSR
    msr_codes = msr_df[portfolio_code_col].unique()  # Unique codes from MSR
    
    # Find matching and non-matching codes
    matching_codes = set(tsr_codes).intersection(set(msr_codes))
    only_in_tsr = set(tsr_codes) - set(msr_codes)
    only_in_msr = set(msr_codes) - set(tsr_codes)
    
    # Calculate statistics
    stats = {
        "Total TSR Codes": len(tsr_codes),
        "Total MSR Codes": len(msr_codes),
        "Matching Codes": len(matching_codes),
        "Only in TSR": len(only_in_tsr),
        "Only in MSR": len(only_in_msr),
        "Percentage Matching": (len(matching_codes) / len(tsr_codes)) * 100 if len(tsr_codes) > 0 else 0,
        "Percentage Non-Matching (TSR or MSR)": ((len(only_in_tsr) + len(only_in_msr)) / (len(tsr_codes) + len(msr_codes))) * 100
    }
    
    return stats, matching_codes, only_in_tsr, only_in_msr

def main():
    # File paths
    tsr_file = 'path/to/tsr.csv'
    msr_file = 'path/to/msr.csv'
    
    # Column names
    uti_col = 'uti'
    portfolio_code_col = 'variationportfoliocode'
    
    # Load and filter TSR file (ignoring NOAP values in UTI column)
    tsr_df = load_and_filter_tsr(tsr_file, uti_col)
    
    # Load MSR file (we only care about the variationportfoliocode column)
    msr_df = pd.read_csv(msr_file)
    
    # Compare variationportfoliocode between TSR and MSR
    stats, matching_codes, only_in_tsr, only_in_msr = compare_variation_portfolio(tsr_df, msr_df, portfolio_code_col)
    
    # Output statistics
    print("Comparison Statistics:")
    for key, value in stats.items():
        print(f"{key}: {value}")
    
    # Optionally, save the results to CSV files
    pd.DataFrame(matching_codes, columns=[portfolio_code_col]).to_csv('matching_codes.csv', index=False)
    pd.DataFrame(only_in_tsr, columns=[portfolio_code_col]).to_csv('only_in_tsr.csv', index=False)
    pd.DataFrame(only_in_msr, columns=[portfolio_code_col]).to_csv('only_in_msr.csv', index=False)

if __name__ == "__main__":
    main()
