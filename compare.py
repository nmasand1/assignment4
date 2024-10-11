import pandas as pd

class CSVComparator:
    def __init__(self, columns):
        self.columns = columns  # List of columns to compare

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

            # Calculate statistics
            total_df1 = len(df1)
            total_df2 = len(df2)
            matching_rows = len(in_both)
            non_matching_rows = len(only_in_df1) + len(only_in_df2)

            stats = {
                "Total Rows in DF1": total_df1,
                "Total Rows in DF2": total_df2,
                "Matching Rows": matching_rows,
                "Non-Matching Rows": non_matching_rows,
                "Percentage Matching": (matching_rows / total_df1) * 100 if total_df1 > 0 else 0,
                "Percentage Non-Matching": (non_matching_rows / (total_df1 + total_df2)) * 100 if (total_df1 + total_df2) > 0 else 0
            }

            print("\nComparison Statistics:")
            for key, value in stats.items():
                print(f"{key}: {value}")

            return only_in_df1, only_in_df2, in_both

        except KeyError as e:
            print(f"KeyError: {e}. One or more specified columns are missing.")
            return None, None, None
        except Exception as e:
            print(f"An error occurred: {e}")
            return None, None, None

# Example usage
if __name__ == "__main__":
    # Load your CSV files
    tsr_file = 'path/to/tsr.csv'  # Update with actual path
    msr_file = 'path/to/msr.csv'  # Update with actual path
    
    # Load the dataframes
    tsr_df = pd.read_csv(tsr_file)
    msr_df = pd.read_csv(msr_file)

    # Filter the TSR DataFrame to exclude rows with 'NOAP' UTI
    filtered_tsr_df = tsr_df[tsr_df['uti'] != 'NOAP']

    # Define the columns to compare
    columns_to_compare = ['uti', 'variationportfoliocode']

    # Create an instance of the CSVComparator
    comparator = CSVComparator(columns_to_compare)

    # Perform the comparison
    only_in_tsr, only_in_msr, in_both = comparator.compare_columns(filtered_tsr_df, msr_df)
