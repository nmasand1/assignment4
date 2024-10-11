import pandas as pd

class CSVComparator:
    def __init__(self, tsr_file, msr_file, columns):
        self.tsr_file = tsr_file
        self.msr_file = msr_file
        self.columns = columns  # List of columns to compare

    def load_and_filter_tsr(self, uti_col):
        """
        Load TSR CSV and filter out rows where UTI has 'NOAP' value.
        """
        tsr_df = pd.read_csv(self.tsr_file)
        # Filter out rows where UTI is 'NOAP'
        filtered_tsr_df = tsr_df[tsr_df[uti_col] != 'NOAP']
        return filtered_tsr_df

    def compare_columns(self, df1, df2):
        try:
            # Ensure the data types for all comparison columns are consistent (string type)
            df1[self.columns] = df1[self.columns].fillna('').astype(str)
            df2[self.columns] = df2[self.columns].fillna('').astype(str)

            # Print the dtypes to confirm consistent data types
            print("Data types in df1 after conversion:")
            print(df1[self.columns].dtypes)
            print("Data types in df2 after conversion:")
            print(df2[self.columns].dtypes)

            # Perform the merge operation
            comparison_df = pd.merge(df1, df2, on=self.columns, how='outer', indicator=True)

            # Get the rows only in df1, only in df2, and present in both
            only_in_df1 = comparison_df[comparison_df['_merge'] == 'left_only']
            only_in_df2 = comparison_df[comparison_df['_merge'] == 'right_only']
            in_both = comparison_df[comparison_df['_merge'] == 'both']

            return only_in_df1, only_in_df2, in_both

        except KeyError as e:
            print(f"KeyError: {e}. One or more specified columns are missing.")
            return None, None, None
        except Exception as e:
            print(f"An error occurred: {e}")
            return None, None, None

    def run_comparison(self, uti_col, portfolio_code_col):
        # Load and filter TSR file (ignoring NOAP values in UTI column)
        tsr_df = self.load_and_filter_tsr(uti_col)

        # Load MSR file
        msr_df = pd.read_csv(self.msr_file)

        # Extract only the relevant columns for comparison
        tsr_filtered = tsr_df[[uti_col, portfolio_code_col]].drop_duplicates()
        msr_filtered = msr_df[[portfolio_code_col]].drop_duplicates()

        # Compare the variationportfoliocode between TSR and MSR
        only_in_tsr, only_in_msr, in_both = self.compare_columns(tsr_filtered, msr_filtered)

        # Calculate statistics
        stats = {
            "Total TSR Codes": len(tsr_filtered),
            "Total MSR Codes": len(msr_filtered),
            "Matching Codes": len(in_both),
            "Only in TSR": len(only_in_tsr),
            "Only in MSR": len(only_in_msr),
            "Percentage Matching": (len(in_both) / len(tsr_filtered)) * 100 if len(tsr_filtered) > 0 else 0,
            "Percentage Non-Matching (TSR or MSR)": ((len(only_in_tsr) + len(only_in_msr)) / 
                                                     (len(tsr_filtered) + len(msr_filtered))) * 100
        }

        # Output statistics
        print("Comparison Statistics:")
        for key, value in stats.items():
            print(f"{key}: {value}")

        # Optionally, save the results to CSV files
        only_in_tsr.to_csv('only_in_tsr.csv', index=False)
        only_in_msr.to_csv('only_in_msr.csv', index=False)
        in_both.to_csv('matching_codes.csv', index=False)

# Example usage
if __name__ == "__main__":
    tsr_file = 'path/to/tsr.csv'  # Update with your actual path
    msr_file = 'path/to/msr.csv'  # Update with your actual path
    uti_column = 'uti'
    portfolio_code_column = 'variationportfoliocode'
    comparison_columns = [portfolio_code_column]  # Columns to compare

    comparator = CSVComparator(tsr_file, msr_file, comparison_columns)
    comparator.run_comparison(uti_column, portfolio_code_column)
