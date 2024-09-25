#!/bin/bash

# Use the current directory
directory=$(pwd)

# Loop through all files in the current directory that start with 'CASH'
for file in ./CASH*; do
    # Check if the file exists
    if [[ -f "$file" ]]; then
        # Get the new file name by replacing 'CASH' with 'NON_CASH'
        newfile="${file/CASH/NON_CASH}"
        # Rename the file
        mv "$file" "$newfile"
        echo "Renamed '$file' to '$newfile'"
    fi
done
