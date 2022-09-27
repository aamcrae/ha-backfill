# ha-backfill

ha-backfill is a utility to backfill energy data to the Home Assistant database
sourced from CSV files.

The CSV file format expected is basically:
```
#date,time,IMP,EXP,GEN-T
2022-04-01,13:00,25077.2,36010.82,59015.335
2022-04-01,13:05,25077.96,36011.61,59018.343
```

Each line is expected to be a 5 minute sample of the total import (energy from the grid),
total export (energy sent to the grid) and solar generation. All values are kWh.

Multiple CSV files are read from the target directory, and the expectation is that
the files are sortable in time order using the filename.

The utility works by deleting the existing records for the relevant fields in the
`statistics` and `statistics_short_term` database tables,
and inserting the values read from the CSV files.

To identify the particular records used for the energy integration, the utility
needs a `metadata_id` string key for the import, export and solar generation statistics.
The easiest way of seeing this is to dump the `statistics_meta` table in the database and
find the sensors that are being referenced in the energy integration e.g:

```
sqlite3 <home-assistant-database>
SQLite version 3.37.2 2022-01-06 13:25:41
Enter ".help" for usage hints.
sqlite> .header on
sqlite> .mode column
sqlite> SELECT * FROM statistics_meta;
id  statistic_id                                  source    unit_of_measurement  has_mean  has_sum  name
--  --------------------------------------------  --------  -------------------  --------  -------  ----
...
13  sensor.export_total                           recorder  kWh                  0         1            
14  sensor.import_total                           recorder  kWh                  0         1            
15  sensor.solar_total                            recorder  kWh                  0         1 
...

```

In this example, the id's are 13, 14 and 15, so these can be set via the flags `export-key`, `import-key` and `gen-key`.

The steps to use this utility are:
- Make appropriate changes to the constants
- go build
- Stop Home Assistant
- `./ha-backfill <flags> | sqlite3 <home-assistant-database>`
- Restart Home Assistant
- Enjoy your updated energy graphs

The utility can be customized by some flags, and also some
constants that may be changed in the code.

This is not an officially supported Google product.
