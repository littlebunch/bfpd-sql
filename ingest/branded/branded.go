// Package branded implements an Ingest for Branded Food Products.
// IMPORTANT: BFPD consists of 2 files: food.csv and branded_food.csv.
// These files need to be sorted prior to running the ingest.
package branded

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/littlebunch/bfpd-sql/ds"
	"github.com/littlebunch/bfpd-sql/ingest"
	bfpd "github.com/littlebunch/bfpd-sql/model"
)

var (
	cnts ingest.Counts
	//gbucket string
)

// Bfpd for implementing the interface
type Bfpd struct {
	Doctype string
}
type line struct {
	id     int
	fields []string // Hold parsed CSV fields instead of raw string
}

// ProcessFiles loads a set of Branded Food Products csv.  The 2 data files (food.csv and
// branded_food.csv) are merged.
func (p Bfpd) ProcessFiles(path string, dc ds.DataSource) error {
	var (
		//dt   *bfpd.DocType
		il   []interface{}
		food bfpd.Food
		err  error
	)
	// read food.csv metadata
	metadataChan := make(chan *line)
	go reader(path+"food.csv", metadataChan)
	// read branded_food.csv details
	brandedChan := make(chan *line)
	go reader(path+"branded_food.csv", brandedChan)
	// merge the two data streams
	mergedLinesChan := make(chan *line)
	go joiner(metadataChan, brandedChan, mergedLinesChan)
	// process the merged stream as csv
	fgid := 0

	for l := range mergedLinesChan {
		// l.fields has been constructed by the joiner from actual CSV fields
		// prepend the ID as the first field since it was stripped during reading
		record := make([]string, len(l.fields)+1)
		record[0] = fmt.Sprintf("%d", l.id)
		copy(record[1:], l.fields)

		// Validate minimum required fields before accessing any
		if len(record) < 25 { // Now including the ID field, need 25 total
			// Log detailed info for problematic records

			log.Printf("Insufficient fields in record (merged from id %d): expected 25, got %d.\n", l.id, len(record))
			continue
		}

		pubdate, err := time.Parse("2006-01-02", record[4])
		if err != nil {
			log.Println(err)
		}
		if dc.FoodExists(record[0]) {
			continue
		}
		food.FdcID = record[0]
		food.Description = record[2]
		food.PublicationDate = pubdate
		food.Brand.Owner = record[8]
		food.Brand.Brand = record[9]
		food.Brand.SubBrand = record[10]
		food.Upc = record[11]
		food.Ingredients = record[12]
		cnts.Foods++
		if cnts.Foods%10000 == 0 {
			log.Println("Foods Count = ", cnts.Foods)
		}
		a, err := strconv.ParseFloat(record[14], 32)
		if err != nil {
			log.Println(record[0] + ": can't parse serving amount " + record[14])
		} else {
			food.Serving.Description = record[16]
			food.Serving.Size = float32(a)
			food.Serving.Unit = record[15]
		}
		food.Datasource = record[19]
		if record[21] != "" {
			food.ModifiedDate, _ = time.Parse("2006-01-02", record[20])
		}
		if record[22] != "" {
			food.AvailableDate, _ = time.Parse("2006-01-02", record[21])
		}
		if record[23] != "" {
			food.DiscontinueDate, _ = time.Parse("2006-01-02", record[23])
		}
		food.Country = record[22]
		if record[18] != "" {
			food.FoodGroup = bfpd.FoodGroup{Description: record[17]}
		} else {
			food.FoodGroup = bfpd.FoodGroup{}
		}
		il = append(il, food)
		fgid++
		if fgid%1000 == 0 {
			log.Println(fgid)
			dc.Create(il)
			il = nil
		}
	}
	if len(il) > 0 {
		dc.Create(il)
	}
	if err = nutrients(path, dc); err != nil {
		log.Printf("nutrient load failed: %v", err)
	}

	log.Printf("Finished.  Counts: %d Foods %d Nutrients\n", cnts.Foods, cnts.Nutrients)
	return err
}

func nutrients(path string, dc ds.DataSource) error {

	fn := path + "food_nutrient.csv"
	f, err := os.Open(fn)
	if err != nil {
		return err
	}
	defer f.Close()
	r := csv.NewReader(f)
	var (
		n []interface{}
	)
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		id := record[1]
		v, err := strconv.ParseInt(record[2], 0, 64)
		if err != nil {
			log.Println(record[0] + ": can't parse nutrient no " + record[1])
		}
		cnts.Nutrients++
		w, err := strconv.ParseFloat(record[3], 32)
		if err != nil {
			log.Println(record[0] + ": can't parse value " + record[3])
		}

		d, err := strconv.ParseInt(record[5], 0, 32)
		if err != nil {
			log.Println(record[5] + ": can't parse derivation no " + record[1])
		}

		n = append(n, bfpd.NutrientData{
			Food:         bfpd.Food{FdcID: id},
			NutrientID:   v,
			Value:        float32(w),
			DerivationID: d,
		})

		if cnts.Nutrients%1000 == 0 {
			log.Println("Nutrients Count = ", cnts.Nutrients)
			err := dc.Create(n)
			if err != nil {
				log.Printf("Bulk insert failed: %v\n", err)
			}
			n = nil
		}

	}

	return nil
}
func reader(fname string, out chan<- *line) {
	defer close(out) // close channel on return

	// open the file
	file, err := os.Open(fname)
	if err != nil {
		log.Fatalf("open: %v", err)
	}
	defer file.Close()

	csvReader := csv.NewReader(file)
	header := true
	lastID := -1
	lineNum := 0
	rowCount := 0
	for {
		record, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("Error reading CSV from %s: %v", fname, err)
		}

		lineNum++

		// ignore first line (header)
		if header {
			header = false
			log.Printf("File %s header: %d fields\n", fname, len(record))
			continue
		}

		// Need at least ID field
		if len(record) < 1 {
			continue
		}

		// convert ID to integer for easier comparison
		id, err := strconv.Atoi(record[0])
		if err != nil {
			log.Printf("Warning: skipping line with non-integer ID '%s' in %s: %v\n", record[0], fname, err)
			continue
		}

		// CHECK: Verify file is sorted by ID (required for joiner algorithm)
		if id < lastID {
			log.Fatalf("ERROR: %s is not sorted by ID.\nIDs must be sorted for the merge to work.\nFound ID %d after %d at line %d.\nPlease sort both files by the ID column (field 0) before running ingest.\n", fname, id, lastID, lineNum)
		}
		lastID = id
		rowCount++

		// Log first few rows and row around problem area (ID 1119794)
		if rowCount <= 3 || id == 1119794 || rowCount%5000 == 0 {
			log.Printf("File %s: row %d, ID %d, fields=%d\n", fname, rowCount, id, len(record)-1)
		}

		// Keep fields as-is (fields 1 onwards) - don't reconstruct as string
		var recordFields []string
		if len(record) > 1 {
			recordFields = record[1:]
		}

		// Allocate a new struct for each line to avoid memory aliasing
		l := &line{
			id:     id,
			fields: recordFields,
		}
		// send the line to the channel
		out <- l
	}
	log.Printf("File %s: processed %d rows\n", fname, rowCount)
}
func joiner(metadata, setIDs <-chan *line, out chan<- *line) {
	defer close(out) // close channel on return

	bf := &line{}
	mdCount := 0
	bfCount := 0
	matchCount := 0

	for md := range metadata {
		mdCount++

		// add matching branded_foods.csv line (if left over from previous iteration)
		if bf.id == md.id {
			md.fields = append(md.fields, bf.fields...)
			matchCount++
		}
		// look for matching branded foods
		for bf = range setIDs {
			bfCount++
			// add all branded_foods.csv with matching IDs
			if bf.id == md.id {
				md.fields = append(md.fields, bf.fields...)
				matchCount++
			} else if bf.id > md.id {
				break
			}
		}
		// send the augmented line into the channel
		out <- md

	}
	log.Printf("Joiner complete: %d metadata, %d branded, %d matches\n", mdCount, bfCount, matchCount)
}
