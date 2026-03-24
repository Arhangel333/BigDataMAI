#!/bin/bash

# Я скрипт который запускает со всеми файлами из $2 скрипт $1

script_file="$1"
csv_dir="$2"

#echo ""$1" "$2""

for file in "$csv_dir"/*.csv; do
    #echo ""$script_file" "$file""
    "$script_file" "$file"
done