// Package branded implements an Ingest for Branded Food Products.
// IMPORTANT: BFPD consists of 2 files: food.csv and branded_food.csv.
// These files need to be sorted prior to running the ingest.
package branded

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/littlebunch/bfpd-sql/ds"
	"github.com/littlebunch/bfpd-sql/ingest"
	bfpd "github.com/littlebunch/bfpd-sql/model"
)

var (
	cnts ingest.Counts
	err  error
	//gbucket string
)

// Bfpd for implementing the interface
type Bfpd struct {
	Doctype string
}
type line struct {
	id         int
	restOfLine string
}
type f struct {
	FdcID string `json:"fdcId" binding:"required"`
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
	var buf bytes.Buffer
	r := csv.NewReader(&buf)
	fgid := 0

	for l := range mergedLinesChan {
		buf.WriteString(fmt.Sprintf("%v,%v", l.id, l.restOfLine))
		record, _ := r.Read()

		pubdate, err := time.Parse("2006-01-02", record[4])
		if err != nil {
			log.Println(err)
		}
		if dc.FoodExists(record[0]) {
			continue
		}
		fgid++
		if fgid%1000 == 0 {
			log.Println(fgid)
			dc.Create(il)
			il = nil
		}
		food.FdcID = record[0]
		food.Description = record[2]
		food.PublicationDate = pubdate
		food.Manufacturer = bfpd.Manufacturer{Name: record[5]}
		food.Upc = record[6]
		food.Ingredients = record[7]
		cnts.Foods++
		if cnts.Foods%10000 == 0 {
			log.Println("Foods Count = ", cnts.Foods)
		}
		a, err := strconv.ParseFloat(record[8], 32)
		if err != nil {
			log.Println(record[0] + ": can't parse serving amount " + record[8])
		} else {
			food.ServingDescription = record[10]
			food.ServingSize = float32(a)
			food.ServingUnit = record[9]
		}
		food.Datasource = record[12]
		if record[13] != "" {
			food.ModifiedDate, _ = time.Parse("2006-01-02", record[13])
		}
		if record[14] != "" {
			food.AvailableDate, _ = time.Parse("2006-01-02", record[14])
		}
		if record[16] != "" {
			food.DiscontinueDate, _ = time.Parse("2006-01-02", record[16])
		}
		food.Country = record[15]
		if record[11] != "" {
			food.FoodGroup = bfpd.FoodGroup{Description: record[11]}
		} else {
			food.FoodGroup = bfpd.FoodGroup{}
		}
		il = append(il, food)

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

	scanner := bufio.NewScanner(file)
	header := true
	for scanner.Scan() {
		var l line
		columns := strings.SplitN(scanner.Text(), ",", 2)
		// ignore first line (header)
		if header {
			header = false
			continue
		}
		// convert ID to integer for easier comparison
		id, err := strconv.Atoi(strings.ReplaceAll(columns[0], "\"", ""))
		if err != nil {
			log.Fatalf("ParseInt: %v", err)
		}
		l.id = id
		l.restOfLine = columns[1]
		// send the line to the channel
		out <- &l
	}
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
}
func joiner(metadata, setIDs <-chan *line, out chan<- *line) {
	defer close(out) // close channel on return

	bf := &line{}
	for md := range metadata {
		sep := ","
		// add matching branded_foods.csv line (if left over from previous iteration)
		if bf.id == md.id {
			md.restOfLine += sep + bf.restOfLine
			sep = " "
		}
		// look for matching branded foods
		for bf = range setIDs {
			// add all branded_foods.csv with matching IDs
			if bf.id == md.id {
				md.restOfLine += sep + bf.restOfLine
				sep = " "
			} else if bf.id > md.id {
				break
			}
		}
		// send the augmented line into the channel
		out <- md
	}
}
