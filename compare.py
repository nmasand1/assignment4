def compare_columns(self, df1, df2):
    try:
        # Cast all comparison columns to string to avoid type mismatch errors
        df1[self.columns] = df1[self.columns].astype(str)
        df2[self.columns] = df2[self.columns].astype(str)

        # Deduplicate dataframes based on the columns to compare
        df1_deduped = df1.drop_duplicates(subset=self.columns)
        df2_deduped = df2.drop_duplicates(subset=self.columns)

        # Perform the merge to find matches and non-matches
        comparison_df = df1_deduped.merge(
            df2_deduped, on=self.columns, how='outer', indicator=True
        )
        
        # Separate into dataframes based on the merge result
        only_in_df1 = comparison_df[comparison_df['_merge'] == 'left_only']
        only_in_df2 = comparison_df[comparison_df['_merge'] == 'right_only']
        in_both = comparison_df[comparison_df['_merge'] == 'both']

        return only_in_df1, only_in_df2, in_both
    except KeyError as e:
        print(f"KeyError: {e}. One or more specified columns are missing.")
        return None, None, None
    except Exception as e:
        print(f"An error occurred during column comparison: {e}")
        return None, None, None





def run_comparison(self):
    try:
        self.load_config()
        dataframes = self.load_csv_files()
        dataframes = self.reorder_files(dataframes)
        
        # Check if any DataFrame is empty or does not exist
        if any(df.empty for df in dataframes.values()):
            raise ValueError("One or more DataFrames are empty or could not be loaded properly.")
        
        if self.check_missing_columns(dataframes):
            keys = list(dataframes.keys())
            only_in_df1, only_in_df2, in_both = self.compare_columns(dataframes[keys[0]], dataframes[keys[1]])

            if only_in_df1 is not None and only_in_df2 is not None:
                result_df = self.create_result_df(only_in_df1, only_in_df2, keys[0], keys[1])
                match_both = self.match_result_df(in_both, keys[0], keys[1])

                self.save_and_print_csv(result_df, 'non_matching_recon_intraday.csv')
                self.save_and_print_csv(match_both, 'matching_row_recon_intraday.csv')

                stats_df = self.calculate_stats(in_both, only_in_df1, only_in_df2)
                self.save_and_print_csv(stats_df, 'comparison_stat_recon_intraday.csv')

                print(f'Matching recon file saved with {in_both.shape[0]} rows.')
                print(f'Non-matching recon saved with {result_df.shape[0]} rows.')
            else:
                print("Comparison could not be completed due to missing columns.")
        else:
            print("One or more specified columns are missing in one or both CSV files.")
    except Exception as e:
        print(f"An error occurred during the comparison process: {e}")
