package mariadb

import (
	"fmt"
	"log"

	"github.com/littlebunch/bfpd-sql/config"
	bfpd "github.com/littlebunch/bfpd-sql/model"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

type SqlDb struct {
	Conn *gorm.DB
}

func (ds *SqlDb) ConnectDs(cs config.Config) error {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:3306)/%s?charset=utf8mb4&parseTime=True&loc=Local", cs.User, cs.Pwd, cs.URL, cs.Db)
	fmt.Printf("Connecting to %s\n", dsn)
	
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		return err
	}
	
	ds.Conn = db
	return nil
}

func (ds *SqlDb) CloseDs() {
	sqlDB, err := ds.Conn.DB()
	if err == nil {
		sqlDB.Close()
	}
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
		createNutrient(r, ds)
	case bfpd.NutrientData:
		createNutrientData(r, ds)
	case bfpd.Derivation:
		createDerivation(r, ds)
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
	return ds.Conn.Transaction(func(tx *gorm.DB) error {
		// Delete nutrient data first (foreign key constraint)
		if err := tx.Delete(&bfpd.NutrientData{}, "food_id = ?", id).Error; err != nil {
			return err
		}
		// Delete the food
		if err := tx.Delete(&bfpd.Food{}, id).Error; err != nil {
			return err
		}
		fmt.Printf("deleted %d\n", id)
		return nil
	})
}

func (ds *SqlDb) RemoveVersions(upc string) error {
	var foods []bfpd.Food
	if err := ds.Conn.Where("upc = ?", upc).Find(&foods).Error; err != nil {
		return err
	}
	
	for _, food := range foods {
		if err := ds.Remove(int64(food.ID)); err != nil {
			log.Printf("Failed to remove %d: %v\n", food.ID, err)
			return err
		}
		log.Printf("Removed %d\n", food.ID)
	}
	
	return nil
}

func checkerr(e error) {
	if e != nil {
		log.Fatal(e)
	}
}

func (ds *SqlDb) FoodExists(fdcid string) bool {
	var food bfpd.Food
	result := ds.Conn.Where("fdc_id = ?", fdcid).First(&food)
	return result.RowsAffected > 0
}

func createFood(foods []interface{}, ds *SqlDb) {
	err := ds.Conn.Transaction(func(tx *gorm.DB) error {
		for _, f := range foods {
			food := f.(bfpd.Food)
			
			// Find or create manufacturer
			manufacturer := bfpd.Manufacturer{Name: food.Manufacturer.Name}
			result := tx.Where("name = ?", food.Manufacturer.Name).FirstOrCreate(&manufacturer)
			if result.Error != nil {
				return result.Error
			}
			food.ManufacturerID = int64(manufacturer.ID)
			
			// Find or create food group
			foodGroup := bfpd.FoodGroup{Description: food.FoodGroup.Description}
			result = tx.Where("description = ?", food.FoodGroup.Description).FirstOrCreate(&foodGroup)
			if result.Error != nil {
				return result.Error
			}
			food.FoodGroupID = int64(foodGroup.ID)
			
			// Create the food
			if result := tx.Create(&food); result.Error != nil {
				return result.Error
			}
		}
		return nil
	})
	
	if err != nil {
		log.Printf("Error creating foods: %v\n", err)
		checkerr(err)
	} else {
		log.Printf("finished with transaction block.")
	}
}

func createNutrient(nutrients []interface{}, ds *SqlDb) {
	err := ds.Conn.Transaction(func(tx *gorm.DB) error {
		for _, n := range nutrients {
			nut := n.(bfpd.Nutrient)
			if result := tx.Create(&nut); result.Error != nil {
				return result.Error
			}
		}
		return nil
	})
	
	if err != nil {
		log.Printf("Error creating nutrients: %v\n", err)
		checkerr(err)
	} else {
		log.Printf("nutrient load finished with transaction block.")
	}
}

func createNutrientData(nutdata []interface{}, ds *SqlDb) {
	err := ds.Conn.Transaction(func(tx *gorm.DB) error {
		for _, n := range nutdata {
			ndata := n.(bfpd.NutrientData)
			
			// Find the food by FdcID
			var food bfpd.Food
			if result := tx.Where("fdc_id = ?", ndata.Food.FdcID).First(&food); result.Error != nil {
				log.Println("No food found for ", ndata.Food.FdcID)
				continue
			}
			
			ndata.FoodID = int64(food.ID)
			if result := tx.Create(&ndata); result.Error != nil {
				return result.Error
			}
		}
		return nil
	})
	
	if err != nil {
		log.Printf("Error creating nutrient data: %v\n", err)
		checkerr(err)
	} else {
		log.Printf("finished with transaction block.")
	}
}

func createDerivation(derivations []interface{}, ds *SqlDb) {
	err := ds.Conn.Transaction(func(tx *gorm.DB) error {
		for _, dv := range derivations {
			derv := dv.(bfpd.Derivation)
			if result := tx.Create(&derv); result.Error != nil {
				return result.Error
			}
		}
		return nil
	})
	
	if err != nil {
		log.Printf("Error creating derivations: %v\n", err)
		checkerr(err)
	} else {
		log.Printf("derivation load finished with transaction block.")
	}
}
