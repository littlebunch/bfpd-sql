package ds

import (
	"fmt"

	"github.com/jinzhu/gorm"
	bfpd "github.com/littlebunch/bfpd-sql/model"
)

type GormDb struct {
	Conn *gorm.DB
}

func (ds *GormDb) ConnectDs(cs bfpd.Config) error {
	c := fmt.Sprintf("%s:%s@tcp(%s:3306)/%s?charset=utf8&parseTime=True&loc=Local", cs.Url, cs.User, cs.Pw, cs.Db)
	//open a db connection
	db, err := gorm.Open("mysql", c)
	if err == nil {
		ds.Conn = db
	}
	return err
}
