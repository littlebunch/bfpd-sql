// Package bfpd implements an Ingest for Branded Food Products.
// IMPORTANT: BFPD consists of 2 files: food.csv and branded_food.csv.
// These files need to be sorted prior to running the ingest.
package bfpd

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/littlebunch/bfpd-sql/ds"
	"github.com/littlebunch/bfpd-sql/ingest"
	"github.com/littlebunch/bfpd-sql/ingest/dictionaries"
	fdc "github.com/littlebunch/bfpd/model"
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
// branded_food.csv) are merged.  Existing foods are ignored and previous versions of a
// food are removed.  The end product is a database containing only current versions of foods
func (p Bfpd) ProcessFiles(path string, dc ds.DataSource, bucket string) error {
	var (
		dt   *fdc.DocType
		il   []interface{}
		food fdc.Food
		s    []fdc.Serving
		err  error
	)

	if il, err = dc.GetDictionary(bucket, dt.ToString(fdc.FGGPC), 0, 500); err != nil {
		return err
	}
	fgrp := dictionaries.InitBrandedFoodGroupInfoMap(il)
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
		fgid++
		if fgid%10000 == 0 {
			log.Println(fgid)
		}
		// ignore existing foods
		if rc := dc.FoodExists(record[0]); rc {
			continue
		} else { // create a new food and remove any previous versions
			s = nil
			pubdate, err := time.Parse("2006-01-02", record[4])
			if err != nil {
				log.Println(err)
			}
			food.ID = record[0]
			food.FdcID = record[0]
			food.Description = record[2]
			food.PublicationDate = pubdate
			food.Manufacturer = record[5]
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
				s = append(s, fdc.Serving{
					Nutrientbasis: record[9],
					Description:   record[10],
					Servingamount: float32(a),
				})
				food.Servings = s
			}
			food.Source = record[12]
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
			food.Type = dt.ToString(fdc.FOOD)
			if record[11] != "" {
				_, fg := fgrp[record[11]]
				if !fg {
					fgid++
					fgrp[record[11]] = fdc.FoodGroup{ID: int32(fgid), Description: record[11], Type: dt.ToString(fdc.FGGPC)}
				}
				food.Group = &fdc.FoodGroup{ID: int32(fgrp[record[11]].ID), Description: fgrp[record[11]].Description, Type: fgrp[record[11]].Type}
			} else {
				food.Group = nil
			}
			// first remove any existing versions for this GTIN/UPC code
			removeVersions(food.Upc, bucket, dc)
			if err = dc.Update(record[0], food); err != nil {
				log.Printf("Update %s failed: %v", record[0], err)
			}

		}

	}
	if err = nutrients(path, bucket, dc); err != nil {
		log.Printf("nutrient load failed: %v", err)
	}

	log.Printf("Finished.  Counts: %d Foods %d Nutrients\n", cnts.Foods, cnts.Nutrients)
	return err
}

// Queries for any foods
func removeVersions(upc string, bucket string, dc ds.DataSource) {

	var (
		r   []interface{}
		fid f
		j   []byte
	)

	q := fmt.Sprintf("SELECT fdcId from %s where upc = \"%s\" AND type=\"FOOD\"", bucket, upc)
	if err := dc.Query(q, &r); err != nil {
		log.Printf("%v\n", err)
		return
	}
	for i := range r {
		if j, err = json.Marshal(r[i]); err != nil {
			log.Printf("%s %v %v\n", upc, j, err)
		}
		if err = json.Unmarshal(j, &fid); err != nil {
			log.Printf("%s %s %v\n", upc, string(j), err)
		}
		log.Printf("Removed %s\n", fid.FdcID)
		if err = dc.Remove(fid.FdcID); err != nil {
			log.Printf("Cannot remove %s\n", fid.FdcID)
		}
	}
	return

}

func nutrients(path string, gbucket string, dc ds.DataSource) error {
	var (
		dt                   *fdc.DocType
		food                 fdc.Food
		n                    []fdc.NutrientData
		cid, source, portion string
		portionValue         float64
	)
	fn := path + "food_nutrient.csv"
	f, err := os.Open(fn)
	if err != nil {
		return err
	}

	r := csv.NewReader(f)
	var (
		//n  []fdc.NutrientData
		il []interface{}
	)
	//q := fmt.Sprintf("select gd.* from %s as gd where type='%s' offset %d limit %d", gbucket, dt.ToString(fdc.NUT), 0, 500)
	if il, err = dc.GetDictionary(gbucket, dt.ToString(fdc.NUT), 0, 500); err != nil {
		return err
	}

	nutmap := dictionaries.InitNutrientInfoMap(il)

	if il, err = dc.GetDictionary(gbucket, dt.ToString(fdc.DERV), 0, 500); err != nil {
		return err
	}
	dlmap := dictionaries.InitDerivationInfoMap(il)
	processit := true
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		id := record[1]
		v, err := strconv.ParseInt(record[2], 0, 32)
		if err != nil {
			log.Println(record[0] + ": can't parse nutrient no " + record[1])
		}
		if processit = dc.FoodExists(id); !processit {
			// delete this record if the parent food doesn't exist
			nid := fmt.Sprintf("%s_%d", id, nutmap[uint(v)].Nutrientno)
			if err = dc.Remove(nid); err != nil {
				log.Printf("Problem with removing %s: %v\n", nid, err)
				continue
			}
		}
		cnts.Nutrients++
		w, err := strconv.ParseFloat(record[3], 64)
		if err != nil {
			log.Println(record[0] + ": can't parse value " + record[4])
		}

		d, err := strconv.ParseInt(record[5], 0, 32)
		if err != nil {
			log.Println(record[5] + ": can't parse derivation no " + record[1])
		}
		var dv *fdc.Derivation
		if dlmap[uint(d)].Code != "" {
			dv = &fdc.Derivation{ID: dlmap[uint(d)].ID, Code: dlmap[uint(d)].Code, Type: dt.ToString(fdc.DERV), Description: dlmap[uint(d)].Description}
		} else {
			dv = nil
		}
		if cid != id {
			if err = dc.Get(id, &food); err != nil {
				log.Printf("Cannot find %s %v", id, err)
			}
			cid = id
			source = food.Source
			portion = food.Servings[0].Description
			portionValue = float64(food.Servings[0].Servingamount)

		}

		n = append(n, fdc.NutrientData{
			ID:           fmt.Sprintf("%s_%d", id, nutmap[uint(v)].Nutrientno),
			FdcID:        id,
			Upc:          food.Upc,
			Description:  food.Description,
			Manufacturer: food.Manufacturer,
			Category:     food.Group.Description,
			Nutrientno:   nutmap[uint(v)].Nutrientno,
			Value:        w,
			Nutrient:     nutmap[uint(v)].Name,
			Unit:         nutmap[uint(v)].Unit,
			Derivation:   dv,
			Type:         dt.ToString(fdc.NUTDATA),
			Source:       source,
			Portion:      portion,
			PortionValue: (portionValue * w) / 100,
		})

		if cnts.Nutrients%1000 == 0 {
			log.Println("Nutrients Count = ", cnts.Nutrients)
			err := dc.Bulk(&n)
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
