// Copyright 2019 Google LLC
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
// The CSV files are assumed to be in a directory tree as:
//  YYYY/
//      MM/
//         YYYY-MM-DD
//
// The MeterMan project generates CSV files of this format.
//
// The relevant columns that are processed are:
// date - to get the date
// time - Only values on the hour are processed
// IMP - Accumlating imported energy (kWh)
// EXP - Accumlating exported energy (kWh)
// GEN-T - Accumlating solar generation (kWh)
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
	"strings"
	"time"
)

var baseDir = flag.String("dir", "/var/cache/MeterMan/csv", "Base directory for CSV files")
var resetTimes = flag.String("reset", "2022-07-25 14:00", "List of times of value resets")

// metadata_id keys for the import, export and solar tables.
// These can obtained from the statistics_meta table in the database
var imp_key = flag.String("14", "import-key", "metadata_id key for import records")
var exp_key = flag.String("13", "export-key", "metadata_id key for export records")
var gen_key = flag.String("15", "gen-key", "metadata_id key for solar generation records")

// Combined format for parsing date/time
const tFmt = "2006-01-02 15:04"

// CSV column headers
const h_date = "#date"
const h_time = "time"
const h_import = "IMP"
const h_export = "EXP"
const h_gen = "GEN-T"

var reset = map[time.Time]bool{}

// One statistical sample
type sample struct {
	t     time.Time // Sample time
	value float32   // value of sample
}

// The set of all samples for one type of statistic
type stat struct {
	last    float32  // Prior sample value (to detect resets)
	skipped int      // Number of skipped samples
	values  []sample // List of samples
}

func main() {
	flag.Parse()

	if len(*resetTimes) != 0 {
		for _, s := range strings.Split(*resetTimes, ",") {
			t, err := time.ParseInLocation(tFmt, s, time.Local)
			if err != nil {
				log.Fatalf("%s: %v", s, err)
			}
			reset[t] = true
		}
	}
	files, err := allFiles(*baseDir)
	if err != nil {
		log.Fatalf("%s: %v", *baseDir, err)
	}
	sort.Strings(files)
	var imp, exp, gen stat
	for _, f := range files {
		err := readFile(f, &imp, &exp, &gen)
		if err != nil {
			log.Printf("%s: %v\n", f, err)
			continue
		}
	}
	imp.generateSQL(*imp_key)
	exp.generateSQL(*exp_key)
	gen.generateSQL(*gen_key)
}

func allFiles(dir string) ([]string, error) {
	var files []string

	err := filepath.Walk(dir,
		func(path string, info os.FileInfo, err error) error {
			if (info.Mode() & os.ModeType) == 0 {
				files = append(files, path)
			}
			return err
		})
	return files, err
}

// readFile reads one CSV file and extracts the hourly summary
func readFile(file string, imp, exp, gen *stat) error {
	f, err := os.Open(file)
	if err != nil {
		return err
	}
	defer f.Close()
	r, err := csv.NewReader(f).ReadAll()
	if err != nil {
		return err
	}
	if len(r) < 2 {
		log.Printf("%s: empty file", file)
		return nil
	}
	// Find columns
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
		if tm.Minute() == 0 {
			imp.addValue(data[impCol], tm)
			exp.addValue(data[expCol], tm)
			gen.addValue(data[genCol], tm)
		}
	}
	return nil
}

func (s *stat) addValue(str string, tm time.Time) {
	f, err := strconv.ParseFloat(str, 64)
	if err == nil && f != 0 {
		if float32(f) < s.last && !reset[tm] {
			// Skip samples that go backwards
			s.skipped++
		} else {
			s.values = append(s.values, sample{tm, float32(f)})
			s.last = float32(f)
		}
	}
}

// generateSQL generates SQL commands to remove old statistic records
// and to insert new records
func (s *stat) generateSQL(key string) {
	fmt.Printf("DELETE FROM statistics WHERE metadata_id = '%s';\n", key)
	var sum float32
	last := s.values[0].value
	tf := "2006-01-02 15:04:05"
	for _, v := range s.values {
		utc := v.t.In(time.UTC)
		// Start date/time is 1 hour before sample time
		start := utc.Add(time.Hour * -1)
		diff := v.value - last
		// Check for reset of value
		if diff < 0 {
			last = v.value
		}
		sum += v.value - last
		last = v.value
		fmt.Printf("INSERT INTO statistics (created, start, state, sum, metadata_id) "+
			"VALUES ('%s', '%s', %f, %f, '%s');\n",
			utc.Format(tf), start.Format(tf), v.value, sum, key)
	}
}
