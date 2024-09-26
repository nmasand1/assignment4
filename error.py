# Convert all columns to lowercase
self.df_on_prem_source.columns = self.df_on_prem_source.columns.str.lower()

# Normalize the key
key = self.properties.source_keys.lower()

# Check for the best matching column (even if there's a slight mismatch)
matching_column = next((col for col in self.df_on_prem_source.columns if key in col), None)

if matching_column:
    self.on_prem_source_list = list(self.df_on_prem_source[matching_column])
else:
    raise KeyError(f"Column {self.properties.source_keys} not found in DataFrame")
