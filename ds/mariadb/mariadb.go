package mariadb

import (
	"fmt"

	"github.com/jinzhu/gorm"
	bfpd "github.com/littlebunch/bfpd-sql/model"
)

type GormDb struct {
	Conn *gorm.DB
}

func (ds *GormDb) ConnectDs(cs bfpd.Config) error {
	c := fmt.Sprintf("%s:%s@tcp(%s:3306)/%s?charset=utf8&parseTime=True&loc=Local", cs.User, cs.Pwd, cs.URL, cs.Db)
	//open a db connection
	db, err := gorm.Open("mysql", c)
	if err == nil {
		ds.Conn = db
	}
	return err
}
func (ds *GormDb) CloseDs() {
	ds.Conn.Close()
}
func (ds *GormDb) InitDb() {
	ds.Conn.AutoMigrate(&bfpd.Food{},
		&bfpd.Nutrient{},
		&bfpd.Manufacturer{},
		&bfpd.NutrientData{},
		&bfpd.Derivation{},
		&bfpd.FoodGroup{},
		&bfpd.Unit{})
}
func (ds *GormDb) Get(q string, f interface{}) error {
	return nil
}
func (ds *GormDb) Query(q string, f *[]interface{}) error {
	return nil
}
func (ds *GormDb) Update(id string, r interface{}) error {
	return nil
}
func (ds *GormDb) Remove(id string) error {
	return nil
}
func (ds *GormDb) FoodExists(id string) bool {
	return true
}
