package main

/*
* Command line utility to bulk load csv representation of bpfd foods.  Uses json decoder, prepared statements and transactions.
* Often requires tweaking innodb_lock_wait_timeout to avoid concurrency deadlock on transaction blocks,e.g > 1000 .
* The default timeout value is rather high -- 5000.
 */
import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"sync"
	"time"

	_ "github.com/jinzhu/gorm/dialects/mariadb"

	bfpd "./model"
)

var wg sync.WaitGroup
var tokens = make(chan struct{}, 200)
var i = flag.String("i", "", "Input JSON file name")
var c = flag.String("c", "config.json", "Config file")
var n = flag.Int("n", 5000, "Number of foods in a transaction")

func main() {
	var cs bfpd.Config
	foods := make(map[int][]bfpd.Food)
	cnt := 0
	g := 0
	flag.Parse()
	if *i == "" {
		log.Fatal("Input file required ")
	}
	raw, err := ioutil.ReadFile(*c)
	if err != nil {
		log.Println(err.Error())
		os.Exit(1)
	}
	json.Unmarshal(raw, &cs)
	ifile, err := os.Open(*i)
	if err != nil {
		log.Fatal("opening input file", err.Error())
		os.Exit(1)
	}
	defer ifile.Close()
	c := fmt.Sprintf("%s:%s@tcp(127.0.0.1:3306)/%s?charset=utf8&parseTime=True&loc=Local", cs.User, cs.Pw, cs.Db)
	//open a db connection
	db, err := sql.Open("mysql", c)
	if err != nil {
		panic("failed to connect database")
	}
	db.SetMaxIdleConns(10)
	db.SetMaxOpenConns(300)
	defer db.Close()

	dec := json.NewDecoder(ifile)

	// read open bracket
	t, err := dec.Token()
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("%s", t)
	// while the array contains values
	for dec.More() {
		var food bfpd.Food
		// decode an array value (Message)
		err := dec.Decode(&food)
		checkerr(err)
		cnt++
		if cnt%*n == 0 {
			log.Printf("%d loaded", cnt)
			go createFood(foods[g], db)
			wg.Add(1)
			g++
		}

		foods[g] = append(foods[g], food)

		//log.Printf("%v: %v\n", food.Ndbno, food.Description)

	}
	wg.Add(1)
	createFood(foods[g], db)
	wg.Wait()
	log.Printf("Finished.  Records loaded %d", cnt)
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
	wd, err := tx.Prepare("insert into weights(version,seq,amount,description,gramweight,datapoints,stddeviation,food_id) values(?,?,?,?,?,?,?,?)")
	checkerr(err)

	fd, err := tx.Prepare("insert into foods(created_at,ndbno,description,food_group_id,ingredients_id,manufacturer_id,datasource) values(?,?,?,(select id from food_groups where cd=?),?,?,?)")
	checkerr(err)
	tokens <- struct{}{}
	for _, f := range foods {
		// check for  manufacturer ID
		err := manq.QueryRow(f.Manufacturer.Name).Scan(&(f.Manufacturer.Id))
		if f.Manufacturer.Id == 0 {
			r, err := man.Exec(time.Now(), f.Manufacturer.Name)
			checkerr(err)
			rid, err := r.LastInsertId()
			checkerr(err)
			f.ManufacturerID = rid
		} else {
			f.ManufacturerID = int64(f.Manufacturer.Id)
		}
		// insert ingredients_id
		/*r, err := ingd.Exec(f.Ingredients.Description, f.Ingredients.Available)
		checkerr(err)
		ingid, err := r.LastInsertId()
		checkerr(err)*/
		// insert food
		//created_at,ndbno,description,food_group_id,ingredients_id,manufacturer_id,datasource
		r, err := fd.Exec(time.Now(), f.Ndbno, f.Description, f.FoodGroup.Cd, ingid, f.ManufacturerID, f.Datasource)
		checkerr(err)
		fid, err := r.LastInsertId()
		checkerr(err)
		// insert Weights
		//version,seq,amount,description,gramweight,datapoints,stddeviation,food_id
		for _, fw := range f.Measures {
			_, err := wd.Exec(0, fw.Seq, fw.Amount, fw.Description, fw.Gramweight, fw.Datapoints, fw.Stddeviation, fid)
			checkerr(err)
		}
		// set our nutrient ID's and Derivation Codes
		for _, item := range f.NutrientData {
			//created_at,value,datapoints,standard_error,add_nut_mark,number_studies,minimum,maximum,degrees_freedom,lower_eb,upper_eb,comment,source_id,derivation_id,nutrient_id,food_id
			_, err := nd.Exec(time.Now(), item.Value,
				item.Datapoints,
				item.StandardError,
				item.AddNutMark,
				item.NumberStudies,
				item.Minimum,
				item.Maximum,
				item.DegreesFreedom,
				item.LowerEB,
				item.UpperEB,
				item.Comment,
				item.Sourcecode.Code,
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
