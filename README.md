# ha-backfill

ha-backfill is a utility to backfill energy data to the Home Assistant database
sourced from CSV files.

The CSV file format expected is basically:
```
#date,time,IMP,EXP,GEN-T
2022-04-01,25077.2,36010.82,59015.335
2022-04-01,25077.96,36011.61,59018.343
```

Multiple CSV files can be read, and the expectation is that
the filenames are sorted in time order. The utility works
by deleting the existing records for the relevant fields in the
statistics database table, and inserting the values read from the CSV files.

The steps to use this utility are:
- Make appropriate changes to the constants
- go build
- Stop Home Assistant
- ./ha-backfill | sqlite3 <home-assistant-database>
- Restart Home Assistant
- Enjoy your updated energy graphs

The utility can be customized by some flags, and also some
costants that may be changed in the code.

This is not an officially supported Google product.
