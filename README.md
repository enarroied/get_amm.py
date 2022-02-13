# get_amm.py

## What this script does
This small script downloads and cleans data from the French Open Data catalog regarding phytopharmaceutical products (AKA pesticides).
The script only selects **organic** pesticides allowed for the use in **vines** against major diseases : mildew and powdery mildew, as well as major pests : grape moth and leafhoppers.

## Why I created this script - please read before using it
This script was created to fill a sheet from a pre-existing Excel file that allows organic winegrowers to record their vine sprayings.
Data extracted with this script is used as the reference in the file. Data should be kept up to date at the very least once a year.
Prior to this script, new data / data updates / data removal was done by hand checking every product from a official website. That task took over a day and was a great source of invalid values.
The way data is returned is aimed to feed that excel sheet, there is no other reason for the table format.
Separators in the CSV files are ";" semicolons since data is in French format with "," commas as decimal separators.

## Why did I use pure python
Because I did not know good libraries when I made this script. I could re-do it in a better way but I have better projects right now, and this script is functional and still used in production as I write this lines (early 2022)
