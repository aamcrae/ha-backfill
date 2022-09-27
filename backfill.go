// Copyright 2022 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// backfill reads the CSV files and exports the
// historical energy values to the Home Assistant database.
// The CSV files are assumed to be in a separate directory.
// A directory walk is used to read the CSV files, which should
// be in time order e.g named as yyyy-mm-dd
// The first line of each file is assumed to be a commented header line e.g
//
//    #date,time,EXP,IMP,GEN-T,...
//
// This header line is used to identify the columns to be used.
//
// The relevant column titles that are processed are:
// date - to get the date
// time - Only values on the hour are processed
// IMP - Accumlating imported energy (kWh)
// EXP - Accumlating exported energy (kWh)
// GEN-T - Accumlating solar generation (kWh)
//
// The MeterMan project generates CSV files of this format.
//
// Once the CSV files are processed, SQL is generated
// that can be applied to the home assistant database.
// The existing records are deleted, and the new statistics added.

package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"
)

var baseDir = flag.String("dir", "/var/cache/MeterMan/csv", "Base directory for CSV files")
var shortTerm = flag.Int("shortterm", 14, "Number of days to to keep short term stats")

// metadata_id keys for the import, export and solar tables.
// These can obtained from the statistics_meta table in the database
var imp_key = flag.String("import-key", "14", "metadata_id key for import records")
var exp_key = flag.String("export-key", "13", "metadata_id key for export records")
var gen_key = flag.String("gen-key", "15", "metadata_id key for solar generation records")

// Format for parsing combined date/time
const tFmt = "2006-01-02 15:04"

// CSV column headers
const h_date = "#date"
const h_time = "time"
const h_import = "IMP"
const h_export = "EXP"
const h_gen = "GEN-T"

// One statistical sample
type sample struct {
	t     time.Time // Sample time
	sum   float32   // Running sum
	value float32   // value of sample
}

// The set of all samples for one statistic
type stat struct {
	last   float32  // Prior sample value (to detect resets)
	total  float32  // Accumulating total
	values []sample // List of samples
}

func main() {
	flag.Parse()

	files, err := getFileNames(*baseDir)
	if err != nil {
		log.Fatalf("%s: %v", *baseDir, err)
	}
	var imp, exp, gen stat
	// Iterate through all the files in time order, and read the CSV data.
	for _, f := range files {
		err := readCSV(f, &imp, &exp, &gen)
		if err != nil {
			log.Printf("%s: %v\n", f, err)
			continue
		}
	}
	imp.generateSQL(*imp_key)
	exp.generateSQL(*exp_key)
	gen.generateSQL(*gen_key)
}

// getFileNames walks the directory and returns all the files,
// in sorted order.
func getFileNames(dir string) ([]string, error) {
	var files []string

	err := filepath.Walk(dir,
		func(path string, info os.FileInfo, err error) error {
			if (info.Mode() & os.ModeType) == 0 {
				files = append(files, path)
			}
			return err
		})
	sort.Strings(files)
	return files, err
}

// readCSV reads one CSV file and extracts the samples
func readCSV(file string, imp, exp, gen *stat) error {
	f, err := os.Open(file)
	if err != nil {
		return err
	}
	defer f.Close()
	r, err := csv.NewReader(f).ReadAll()
	if err != nil {
		return err
	}
	// File must contain at least a header line and one line of data
	if len(r) < 2 {
		log.Printf("%s: empty file", file)
		return nil
	}
	// Find columns in header line
	dateCol := -1
	timeCol := -1
	impCol := -1
	expCol := -1
	genCol := -1
	for i, s := range r[0] {
		switch s {
		case h_date:
			dateCol = i
			break

		case h_time:
			timeCol = i
			break

		case h_import:
			impCol = i
			break

		case h_export:
			expCol = i
			break

		case h_gen:
			genCol = i
			break
		}
	}
	if dateCol == -1 || timeCol == -1 {
		log.Printf("%s: cannot find date or time", file)
		return nil
	}
	// Iterate through the records
	for i, data := range r[1:] {
		var err error

		if len(data) != len(r[0]) {
			log.Printf("%s: %d: Mismatch in column count", file, i+1)
			continue
		}
		t := data[dateCol] + " " + data[timeCol]
		tm, err := time.ParseInLocation(tFmt, t, time.Local)
		if err != nil {
			log.Printf("%s: %d: Cannot parse date (%s)", file, i+1, t)
			continue
		}
		if impCol != -1 {
			imp.addValue(data[impCol], tm)
		}
		if expCol != -1 {
			exp.addValue(data[expCol], tm)
		}
		if genCol != -1 {
			gen.addValue(data[genCol], tm)
		}
	}
	return nil
}

// addValue will append one value to this stat's list of values.
func (s *stat) addValue(str string, tm time.Time) {
	f, err := strconv.ParseFloat(str, 64)
	val := float32(f)
	if err == nil && f != 0 {
		if len(s.values) == 0 || val < s.last {
			// Reset base if first item or value has gone backwards
			s.last = val
		}
		s.total += val - s.last
		s.values = append(s.values, sample{tm, s.total, val})
		s.last = val
	}
}

// generateSQL generates SQL commands to remove old statistic records
// and to insert new records
func (s *stat) generateSQL(key string) {
	fmt.Printf("DELETE FROM statistics WHERE metadata_id = '%s';\n", key)
	fmt.Printf("DELETE FROM statistics_short_term WHERE metadata_id = '%s';\n", key)
	one_hour := time.Minute * -60
	five_min := time.Minute * -5
	short_term := time.Now().In(time.UTC).Add(-time.Hour * 24 * time.Duration(*shortTerm))
	for _, v := range s.values {
		utc := v.t.In(time.UTC)
		if utc.Minute() == 0 {
			v.insert("statistics", utc, one_hour, key)
		}
		if utc.After(short_term) {
			v.insert("statistics_short_term", utc, five_min, key)
		}
	}
}

// insert generates the SQL to insert a record into the selected table
func (v *sample) insert(table string, tm time.Time, offset time.Duration, key string) {
	const tf = "2006-01-02 15:04:05"
	// Start date/time is 1 sample time before create time.
	// Create time is offset by 10 seconds (to match what home assistant recorder does)
	start := tm.Add(offset)
	fmt.Printf("INSERT INTO %s (created, start, state, sum, metadata_id) "+
		"VALUES ('%s', '%s', %f, %f, '%s');\n",
		table, tm.Add(time.Second*10).Format(tf), start.Format(tf), v.value, v.sum, key)
}
