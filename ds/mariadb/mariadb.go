package mariadb

import (
	"database/sql"
	"fmt"
	"log"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/littlebunch/bfpd-sql/config"
	bfpd "github.com/littlebunch/bfpd-sql/model"
)

type SqlDb struct {
	Conn *sql.DB
}

func (ds *SqlDb) ConnectDs(cs config.Config) error {
	c := fmt.Sprintf("%s:%s@tcp(%s:3306)/%s?charset=utf8&parseTime=True&loc=Local", cs.User, cs.Pwd, cs.URL, cs.Db)
	//open a db connection
	db, err := sql.Open("mysql", c)
	if err == nil {
		ds.Conn = db
	}
	return err
}
func (ds *SqlDb) CloseDs() {
	ds.Conn.Close()
}

func (ds *SqlDb) Get(q string, f interface{}) error {
	return nil
}
func (ds *SqlDb) Query(q string, f *[]interface{}) error {
	return nil
}
func (ds *SqlDb) Create(r []interface{}) error {
	switch t := r[0].(type) {
	case bfpd.Food:
		createFood(r, ds.Conn)
	case bfpd.Nutrient:
		createNutrient(r, ds.Conn)
	default:
		fmt.Println("Unknown type!", t)
	}
	return nil
}
func (ds *SqlDb) GetDictionary(dt string, offset int32, max int32) ([]interface{}, error) {
	var r []interface{}
	return r, nil
}
func (ds *SqlDb) Remove(id string) error {
	return nil
}
func (ds *SqlDb) FoodExists(id string) bool {
	return true
}
func createFood(foods []interface{}, db *sql.DB) {
	var wg sync.WaitGroup
	var tokens = make(chan struct{}, 200)
	defer wg.Done()
	// get manufacturer id
	tx, err := db.Begin()
	checkerr(err)
	defer tx.Rollback()
	manq, err := tx.Prepare("select id from manufacturers where name=?")
	checkerr(err)
	man, err := tx.Prepare("insert into manufacturers(created_at,name) values(?,?)")
	checkerr(err)
	fd, err := tx.Prepare("insert into foods(created_at,fdc_id,description,food_group_id,ingredients_id,manufacturer_id,datasource,upc,publication_date,modified_date,available_date,discontinue_date,serving_size,serving_unit,serving_description) values(?,?,?,(select id from food_groups where cd=?),?,?,?,?,?,?,?,?,?,?,?,?)")
	checkerr(err)
	tokens <- struct{}{}
	for _, f := range foods {
		// check for  manufacturer ID
		food := f.(bfpd.Food)
		err := manq.QueryRow(food.Manufacturer.Name).Scan(&(food.ManufacturerID))
		if food.ManufacturerID == 0 {
			r, err := man.Exec(time.Now(), food.Manufacturer.Name)
			checkerr(err)
			rid, err := r.LastInsertId()
			checkerr(err)
			food.ManufacturerID = rid
		}
		_, err = fd.Exec(time.Now(),
			food.FdcID,
			food.Description,
			food.FoodGroupID,
			food.Ingredients,
			food.ManufacturerID,
			food.Datasource,
			food.Upc,
			food.PublicationDate,
			food.ModifiedDate,
			food.AvailableDate,
			food.DiscontinueDate,
			food.ServingSize,
			food.ServingUnit,
			food.ServingDescription,
			food.Country)
		checkerr(err)
		//fid, err := r.LastInsertId()
		//checkerr(err)
		// set our nutrient ID's and Derivation Codes
		/*for _, item := range f.NutrientData {
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
		}*/

	}
	checkerr(tx.Commit())
	log.Printf("finished with transaction block.")
	<-tokens
}
func createNutrient(nutrients []interface{}, db *sql.DB) {
	tx, err := db.Begin()
	checkerr(err)

	nd, err := tx.Prepare("insert into nutrients(nutrientno,description,unit) values(?,?,?)")
	checkerr(err)
	for _, n := range nutrients {
		nut := n.(bfpd.Nutrient)
		nd.Exec(nut.Nutrientno, nut.Description, nut.Unit)
		checkerr(err)
	}
	checkerr(tx.Commit())
	log.Printf("nutrient load finished with transaction block.")
}
func createNutData(foods []interface{}, db *sql.DB) {

}
func checkerr(e error) {
	if e != nil {
		log.Fatal(e)
	}
}
