//Package dictionaries implements an Ingest for supporting files, e.g. Nutrients
package dictionaries

import (
	"encoding/csv"
	"io"
	"log"
	"os"
	"strconv"

	"github.com/littlebunch/bfpd-sql/ds"
	bfpd "github.com/littlebunch/bfpd-sql/model"
)

//Dictionary for implementing the interface
type Dictionary struct {
	Dt bfpd.DocType
}

// ProcessFiles implements an Ingest of Dictionary objects
func (p Dictionary) ProcessFiles(path string, dc ds.DataSource) error {
	cnt := 0
	var r []interface{}
	f, err := os.Open(path)
	if err != nil {
		log.Printf("Cannot open %s", path)
		return err
	}
	csv := csv.NewReader(f)
	for {
		record, err := csv.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("%v", err)
			return err
		}
		cnt++
		if cnt == 1 {
			continue
		}
		id, err := strconv.ParseInt(record[0], 10, 0)
		if err != nil {
			id = 0
		}
		switch p.Dt {
		// derivation codes
		case bfpd.DERV:

			r = append(r, bfpd.Derivation{
				ID:          int32(id),
				Code:        record[1],
				Description: record[2],
			})
		// nutrients
		case bfpd.NUT:
			r = append(r,
				bfpd.Nutrient{
					ID:          int32(id),
					Nutrientno:  record[3],
					Description: record[1],
					Unit:        record[2],
				})
		}
	}
	dc.Create(r)
	return nil
}
