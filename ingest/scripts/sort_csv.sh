#!/bin/bash
# This script sorts the food.csv and branded_food.csv files 
#by their first column (ID) while preserving the header row. 

# For food.csv
tail -n +2 food.csv | sort -t, -k1 -n | cat <(head -n 1 food.csv) - > food.csv.sorted
mv food.csv.sorted food.csv

# For branded_food.csv
tail -n +2 branded_food.csv | sort -t, -k1 -n | cat <(head -n 1 branded_food.csv) - > branded_food.csv.sorted
mv branded_food.csv.sorted branded_food.csv