# Big data

Example code for chapter five, [Clojure for Data Science](https://www.packtpub.com/big-data-and-business-intelligence/clojure-data-science).

## Data

Data sourced from the IRS on 2012 US Zip code AGI statistics for all zip codes (12zpallagi.csv). The Statistics of Income (SOI) division bases its ZIP code data on administrative records of individual income tax returns (Forms 1040) from the Internal Revenue Service (IRS) Individual Master File (IMF) system. More information is available [here](https://www.datadives.com/table.php?tableid=tb_10097).

The data can be downloaded directly from [here](http://www.irs.gov/pub/irs-soi/12zpallagi.csv).

## Instructions

### *nix and OS X

Run the following command-line script to download the data to the project's data directory:

```bash
# Downloads and unzips the data files into this project's data directory.
    
script/download-data.sh
```

### Windows / manual instructions

  1. Download 12zpallagi.csv into this project's data directory using the link above
  2. Rename the file 12zpallagi.csv to soi.csv

## Running examples

Examples can be run with:
```bash
# Replace 5.1 with the example you want to run:

lein run -e 5.1
```
or open an interactive REPL with:

```bash
lein repl
```

## License

Copyright © 2015 Henry Garner

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
