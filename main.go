package main

/*
* Command line utility to bulk load csv representation of bpfd foods.  Uses json decoder, prepared statements and transactions.
* Often requires tweaking innodb_lock_wait_timeout to avoid concurrency deadlock on transaction blocks,e.g > 1000 .
* The default timeout value is rather high -- 5000.
 */
import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	_ "github.com/jinzhu/gorm/dialects/mysql"

	"github.com/littlebunch/bfpd-sql/ds/mariadb"
	bfpd "github.com/littlebunch/bfpd-sql/model"
)

var wg sync.WaitGroup
var tokens = make(chan struct{}, 200)
var a = flag.Bool("a", false, "Create schema")
var i = flag.String("i", "", "Input JSON file name")
var c = flag.String("c", "config.yaml", "YAML config file")
var t = flag.String("t", "FOOD", "Type of data to ingest: NUT, DERV, GROUP, FOOD")
var n = flag.Int("n", 5000, "Number of foods in a transaction")

func main() {
	var (
		cs    bfpd.Config
		dtype string
		//in    ingest.Ingest
		mdb mariadb.GormDb
	)
	flag.Parse()
	if *i == "" {
		log.Fatal("CSV Input path is required ")
	}

	switch *t {
	case "FOOD":
		dtype = *t
	case "NUT":
		dtype = *t
	case "DERV":
		dtype = *t
	case "GROUP":
		dtype = *t
	default:
		log.Fatal("Invalid -t option ", *t)
		os.Exit(1)
	}
	fmt.Printf("dtype is %s\n", dtype)
	err := cs.GetConfig(c)
	if err != nil {
		log.Fatal("Cannot process config: ", err.Error())
	}
	ds := &mdb
	err = ds.ConnectDs(cs)
	defer ds.CloseDs()
	if err != nil {
		log.Fatal("failed to connect to database ", err.Error())
	}
	db := mdb.Conn
	db.DB().SetMaxIdleConns(10)
	db.DB().SetMaxOpenConns(300)
	if *a {
		ds.InitDb()
	}
	os.Exit(0)
}
func createFood(foods []bfpd.Food, db *sql.DB) {

	defer wg.Done()
	// get manufacturer id
	tx, err := db.Begin()
	checkerr(err)
	defer tx.Rollback()
	manq, err := tx.Prepare("select id from manufacturers where name=?")
	checkerr(err)
	// insert nutrient data
	nd, err := tx.Prepare("insert into nutrient_data(created_at,value,datapoints,standard_error,add_nut_mark,number_studies,minimum,maximum,degrees_freedom,lower_eb,upper_eb,comment,source_id,derivation_id,nutrient_id,food_id) values(?,?,?,?,?,?,?,?,?,?,?,?,(select id from source_codes where code=?),(select id from derivations where code=?),(select id from nutrients where nutrientno=?),?)")
	if err != nil {
		log.Fatal("BAD!", err)
	}
	man, err := tx.Prepare("insert into manufacturers(created_at,name) values(?,?)")
	checkerr(err)
	//wd, err := tx.Prepare("insert into weights(version,seq,amount,description,gramweight,datapoints,stddeviation,food_id) values(?,?,?,?,?,?,?,?)")
	//checkerr(err)

	fd, err := tx.Prepare("insert into foods(created_at,ndbno,description,food_group_id,ingredients_id,manufacturer_id,datasource) values(?,?,?,(select id from food_groups where cd=?),?,?,?)")
	checkerr(err)
	tokens <- struct{}{}
	for _, f := range foods {
		// check for  manufacturer ID
		err := manq.QueryRow(f.Manufacturer.Name).Scan(&(f.ManufacturerID))
		if f.ManufacturerID == 0 {
			r, err := man.Exec(time.Now(), f.Manufacturer.Name)
			checkerr(err)
			rid, err := r.LastInsertId()
			checkerr(err)
			f.ManufacturerID = rid
		}
		r, err := fd.Exec(time.Now(), f.FdcID, f.Description, f.FoodGroup.Cd, f.Ingredients, f.ManufacturerID, f.Datasource)
		checkerr(err)
		fid, err := r.LastInsertId()
		checkerr(err)
		// set our nutrient ID's and Derivation Codes
		for _, item := range f.NutrientData {
			//created_at,value,datapoints,standard_error,add_nut_mark,number_studies,minimum,maximum,degrees_freedom,lower_eb,upper_eb,comment,source_id,derivation_id,nutrient_id,food_id
			_, err := nd.Exec(time.Now(), item.Value,
				item.Datapoints,
				item.StandardError,
				item.NumberStudies,
				item.Minimum,
				item.Maximum,
				item.Derivation.Code,
				item.NutrientID,
				fid)
			if err != nil {
				log.Fatal("nd ", err)
			}
		}

	}
	checkerr(tx.Commit())
	log.Printf("finished with transaction block.")
	<-tokens
}
func checkerr(e error) {
	if e != nil {
		log.Fatal(e)
	}
}
