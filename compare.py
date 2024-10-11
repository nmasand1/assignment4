def compare_columns(self, df1, df2):
    try:
        # Convert all relevant columns to strings and handle NaN values
        df1[self.columns] = df1[self.columns].fillna('').astype(str).apply(lambda x: x.str.strip().str.lower())
        df2[self.columns] = df2[self.columns].fillna('').astype(str).apply(lambda x: x.str.strip().str.lower())

        # Drop duplicates to make the comparison more efficient
        df1_deduped = df1.drop_duplicates(subset=self.columns)
        df2_deduped = df2.drop_duplicates(subset=self.columns)
        
    except KeyError as e:
        print(f"KeyError: {e}. One or more specified columns are missing.")
        return None, None, None

    # Perform the merge operation
    comparison_df = pd.merge(df1_deduped, df2_deduped, on=self.columns, how='outer', indicator=True)

    # Get the rows only in df1, only in df2, and present in both
    only_in_df1 = comparison_df[comparison_df['_merge'] == 'left_only']
    only_in_df2 = comparison_df[comparison_df['_merge'] == 'right_only']
    in_both = comparison_df[comparison_df['_merge'] == 'both']

    return only_in_df1, only_in_df2, in_both
