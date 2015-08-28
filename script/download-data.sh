#!/bin/bash

script_dir=$(cd -P -- "$(dirname -- "$0")" && pwd -P)
data_dir="${script_dir}/../data"
soi_url="http://www.irs.gov/pub/irs-soi/12zpallagi.csv"

mkdir -p "${data_dir}"

echo "Downloading ${soi_url}..."
if [ $(curl -s --head -w %{http_code} $soi_url -o /dev/null) -eq 200 ]; then
    curl -o "${data_dir}/soi.csv" $soi_url
else
    echo "Couldn't download data. Perhaps it has moved? Consult http://wiki.clojuredatascience.com"
fi
