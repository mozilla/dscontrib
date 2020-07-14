#! /usr/local/bin/xonsh

$project="moz-fx-data-bq-data-science"
$dataset="wbeard"

# tables=$(bq ls "$project:$dataset" | awk '{print $1}' | tail +3)
tables=$(bq ls "$project:$dataset")

import re

for table in tables.splitlines():
    ts = table.split() + ['', '']
    table_name, TABLE, *_ = ts
    if TABLE != 'TABLE':
        continue
    if not table_name.startswith('2019'):
        continue
    full_table_name = f"{$project}:{$dataset}.{table_name}"
    $cmd = f"bq rm -f {full_table_name}"
    echo $cmd
    # bq rm -f @(full_table_name)
