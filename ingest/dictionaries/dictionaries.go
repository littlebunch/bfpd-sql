//Package dictionaries implements an Ingest for supporting files
package dictionaries

import (
	"encoding/csv"
	"io"
	"log"
	"os"
	"strconv"

	"github.com/littlebunch/fdc-api/ds"
	fdc "github.com/littlebunch/fdc-api/model"
)

//Dictionary for implementing the interface
type Dictionary struct {
	Dt fdc.DocType
}

// ProcessFiles implements an Ingest of Dictionary objects
func (p Dictionary) ProcessFiles(path string, dc ds.DataSource, bucket string) error {
	t := p.Dt.ToString(p.Dt)
	cnt := 0
	f, err := os.Open(path)
	if err != nil {
		log.Printf("Cannot open %s", path)
		return err
	}
	r := csv.NewReader(f)
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("%v", err)
			return err
		}
		cnt++
		switch p.Dt {
		// derivation codes
		case fdc.DERV:
			id, err := strconv.ParseInt(record[0], 10, 0)
			if err != nil {
				id = 0
			}
			dc.Update(t+":"+record[0],
				fdc.Derivation{
					ID:          int32(id),
					Code:        record[1],
					Type:        t,
					Description: record[2],
				})
		// Standard release food groups
		case fdc.FGSR:
			no, err := strconv.ParseInt(record[0], 10, 0)
			if err != nil {
				no = 0
			}
			dc.Update(t+":"+record[0],
				fdc.FoodGroup{
					ID:          int32(no),
					Code:        record[1],
					Type:        t,
					Description: record[2],
					//LastUpdate:  record[3],
				})
		// FNDDS food groups
		case fdc.FGFNDDS:
			no, err := strconv.ParseInt(record[0], 10, 0)
			if err != nil {
				no = 0
			}
			dc.Update(t+":"+record[0],
				fdc.FoodGroup{
					ID:          int32(no),
					Type:        t,
					Description: record[1],
				})

		// nutrients
		case fdc.NUT:
			var nid int64
			/*no, err := strconv.ParseFloat(record[3], 32)
			if err != nil {
				fmt.Printf("%v\n", err)
				no = 0
			}*/
			nid, err = strconv.ParseInt(record[0], 10, 0)
			if err != nil {
				log.Println("Cannot parse ID: " + record[0])
				continue
			}
			err = dc.Update(t+":"+record[0],
				fdc.Nutrient{
					NutrientID: uint(nid),
					Nutrientno: record[3],
					Name:       record[1],
					Unit:       record[2],
					Type:       t,
				})
			if err != nil {
				log.Printf("Cannot update dictionary %v\n", err)
			}

		}
	}
	return nil
}

// InitNutrientInfoMap creates a map from NUT documents in the data store.
func InitNutrientInfoMap(il []interface{}) map[uint]fdc.Nutrient {
	m := make(map[uint]fdc.Nutrient)
	for _, v := range il {
		n := v.(fdc.Nutrient)
		m[uint(n.NutrientID)] = n
	}
	return m
}

// InitDerivationInfoMap creates a map from DERV documents in the data store.
func InitDerivationInfoMap(il []interface{}) map[uint]fdc.Derivation {
	m := make(map[uint]fdc.Derivation)
	for _, v := range il {
		d := v.(fdc.Derivation)
		m[uint(d.ID)] = d
	}
	return m
}

// InitFoodGroupInfoMap creates a map from FGSR or FGFNDDS documents in the data store
func InitFoodGroupInfoMap(il []interface{}) map[uint]fdc.FoodGroup {
	m := make(map[uint]fdc.FoodGroup)
	for _, v := range il {
		fg := v.(fdc.FoodGroup)
		m[uint(fg.ID)] = fg
	}
	return m
}

// InitBrandedFoodGroupInfoMap creates a map for FGGPC documents
func InitBrandedFoodGroupInfoMap(il []interface{}) map[string]fdc.FoodGroup {
	m := make(map[string]fdc.FoodGroup)
	for _, v := range il {
		fg := v.(fdc.FoodGroup)
		m[fg.Description] = fg
	}

	return m
}
