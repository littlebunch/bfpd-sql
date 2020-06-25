package mariadb

import (
	"database/sql"
	"fmt"
	"log"

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
		createFood(r, ds)
	case bfpd.Nutrient:
		createNutrient(r, ds.Conn)
	case bfpd.NutrientData:
		createNutrientData(r, ds.Conn)
	case bfpd.Derivation:
		createDerivation(r, ds.Conn)
	default:
		fmt.Println("Unknown type!", t)
	}
	return nil
}
func (ds *SqlDb) GetDictionary(dt string, offset int32, max int32) ([]interface{}, error) {
	var r []interface{}
	return r, nil
}
func (ds *SqlDb) Remove(id int64) error {
	var err error

	tx, err := ds.Conn.Begin()
	f, err := tx.Prepare("delete from foods where id = ?")
	n, err := tx.Prepare("delete from nutrient_data where food_id=?")
	if _, err = f.Exec(id); err == nil {
		_, err = n.Exec(id)
		fmt.Printf("deleted %d\n", id)
	}
	return tx.Commit()
}
func (ds *SqlDb) RemoveVersions(upc string) error {
	var (
		err  error
		rows *sql.Rows
	)
	db := ds.Conn
	q := fmt.Sprintf("SELECT id from foods where upc = \"%s\" ", upc)
	if rows, err = db.Query(q, nil); err != nil {
		return err
	}
	for rows.Next() {
		var id sql.NullInt64
		if err = rows.Scan(&id); err != nil {
			return err
		}
		if id.Valid {
			i, _ := id.Value()
			err = ds.Remove(i.(int64))
			log.Printf("Removed %d\n", i)
		}

	}
	return err

}
func checkerr(e error) {
	if e != nil {
		log.Fatal(e)
	}
}

func (ds *SqlDb) FoodExists(fdcid string) bool {
	var id int32
	ds.Conn.QueryRow("SELECT id from foods where fdc_id=?", fdcid).Scan(&id)
	return id != 0

}
func createFood(foods []interface{}, ds *SqlDb) {
	db := ds.Conn
	tx, err := db.Begin()
	checkerr(err)
	defer tx.Rollback()
	checkerr(err)
	manq, err := tx.Prepare("select id from manufacturers where name=?")
	checkerr(err)
	man, err := tx.Prepare("insert into manufacturers(name) values(?)")
	checkerr(err)
	fd, err := tx.Prepare("insert into foods(fdc_id,description,food_group_id,ingredients,manufacturer_id,datasource,upc,publication_date,modified_date,available_date,discontinue_date,serving_size,serving_unit,serving_description,country) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
	checkerr(err)
	fgq, err := tx.Prepare("select id from food_groups where description=?")
	checkerr(err)
	fg, err := tx.Prepare("insert into food_groups(description) values(?)")
	checkerr(err)
	for _, f := range foods {
		// check for  manufacturer ID
		food := f.(bfpd.Food)

		err := manq.QueryRow(food.Manufacturer.Name).Scan(&(food.ManufacturerID))
		if food.ManufacturerID == 0 {
			r, err := man.Exec(food.Manufacturer.Name)
			checkerr(err)
			rid, err := r.LastInsertId()
			checkerr(err)
			food.ManufacturerID = rid
		}
		err = fgq.QueryRow(food.FoodGroup.Description).Scan(&(food.FoodGroupID))
		if food.FoodGroupID == 0 {
			r, err := fg.Exec(food.FoodGroup.Description)
			checkerr(err)
			fgid, err := r.LastInsertId()
			checkerr(err)
			food.FoodGroupID = fgid
		}
		if food.ManufacturerID == 0 {
			r, err := man.Exec(food.Manufacturer.Name)
			checkerr(err)
			rid, err := r.LastInsertId()
			checkerr(err)
			food.ManufacturerID = rid
		}

		_, err = fd.Exec(
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

	}
	checkerr(tx.Commit())
	log.Printf("finished with transaction block.")
}
func createNutrient(nutrients []interface{}, db *sql.DB) {
	tx, err := db.Begin()
	checkerr(err)

	nd, err := tx.Prepare("insert into nutrients(id,nutrientno,description,unit) values(?,?,?,?)")
	checkerr(err)
	for _, n := range nutrients {
		nut := n.(bfpd.Nutrient)
		_, err := nd.Exec(nut.ID, nut.Nutrientno, nut.Description, nut.Unit)
		checkerr(err)
	}
	checkerr(tx.Commit())
	log.Printf("nutrient load finished with transaction block.")
}
func createNutrientData(nutdata []interface{}, db *sql.DB) {
	tx, err := db.Begin()
	checkerr(err)
	nd, err := tx.Prepare("insert into nutrient_data (food_id,nutrient_id,derivation_id,value) values(?,?,?,?)")
	fd, err := tx.Prepare("select id from foods where fdc_id=?")
	checkerr(err)
	for _, n := range nutdata {
		ndata := n.(bfpd.NutrientData)
		err := fd.QueryRow(ndata.Food.FdcID).Scan(&(ndata.FoodID))
		if ndata.FoodID == 0 {
			log.Println("No food id for ", ndata.Food.FdcID)
			continue
		}
		_, err = nd.Exec(ndata.FoodID, ndata.NutrientID, ndata.DerivationID, ndata.Value)
		checkerr(err)
	}
	checkerr(tx.Commit())
	log.Printf("finished with transaction block.")
}
func createDerivation(derivations []interface{}, db *sql.DB) {
	tx, err := db.Begin()
	checkerr(err)
	d, err := tx.Prepare("insert into derivations(id,code,description) values(?,?,?)")
	checkerr(err)
	for _, dv := range derivations {
		derv := dv.(bfpd.Derivation)
		_, err := d.Exec(derv.ID, derv.Code, derv.Description)
		checkerr(err)
	}
	checkerr(tx.Commit())
	log.Printf("derivation load finished with transaction block.")
}

