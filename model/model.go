// types for the bfpd database model
package bfpd

import (
	"time"

	"github.com/jinzhu/gorm"
)

// Food reflects JSON used to transfer BFPD foods data from USDA csv
type Food struct {
	gorm.Model
	PublicationDate    time.Time `json:"publicationDateTime"`
	ModifiedDate       time.Time `json:"modifiedDate,omitempty"`
	AvailableDate      time.Time `json:"availableDate,omitempty"`
	DiscontinueDate    time.Time `json:"discontinueDate,omitempty"`
	Upc                string    `json:"upc" binding:"required" gorm:"unique;not null"`
	FdcID              string    `json:"fdcId" binding:"required"`
	Description        string    `json:"name" binding:"required"`
	FoodGroup          FoodGroup `gorm:"ForeignKey:FoodGroupID" json:"fg"`
	FoodGroupID        int64
	Ingredients        string
	Manufacturer       Manufacturer `json:"company" gorm:"foreignkey:ManufacturerID"`
	ManufacturerID     int64
	Datasource         string         `json:"source"`
	NutrientData       []NutrientData `json:"nutrients" gorm:"foreignkey:FoodID"`
	ServingSize        float32        `json:"value,omitempty"`
	ServingUnit        string         `json:"servingUnit"`
	ServingDescription string         `json:"servingDescription"`
	Country            string         `json:"marketCountry,omitempty"`
}

type Nutrient struct {
	gorm.Model
	Version      uint8
	Nutrientno   uint   `json:"nutno" binding:"required" gorm:"unique;not null"`
	Tagname      string `json:"tag"`
	Description  string `json:"desc" gorm:"not null"`
	Decimalpoint uint8  `json:"dp"`
	Srnutorder   uint32 `json:"sort"`
	Unit         Unit   `gorm:"ForeignKey:UnitID"`
	UnitID       uint
	Type         string

	NutrientData []NutrientData `gorm:"ForeignKey:NutrientID"`
}
type Manufacturer struct {
	gorm.Model
	Version uint8
	Name    string `json:"name" binding:"required"`
	Foods   []Food
}

type Unit struct {
	gorm.Model
	Version   uint8  `json:"version"`
	Unit      string `gorm:"unique;not null" json:"unit"`
	Nutrients []Nutrient
}
type SourceCode struct {
	gorm.Model
	Code         string         `binding:"required" json:"code"`
	Description  string         `json:"desc"`
	NutrientData []NutrientData //`gorm:"ForeignKey:SourceID"`
}
type Derivation struct {
	gorm.Model
	Code         string `binding:"required" json:"code"`
	Description  string `json:"desc"`
	NutrientData []NutrientData
}
type FoodGroup struct {
	gorm.Model
	Cd          string `json:"cd" gorm:"unique;not null"`
	Description string `json:"desc"`
	Food        []Food
}
type NutrientData struct {
	gorm.Model
	Value         float32 `json:"value"`
	Datapoints    uint32  `json:"dp"`
	StandardError float32 `json:"se"`
	AddNutMark    string
	NumberStudies uint8
	Minimum       float32
	Maximum       float32
	Median        float32
	Derivation    Derivation `json:"deriv" gorm:"ForeignKey:DerivationID"`
	DerivationID  int64
	Nutrient      Nutrient `gorm:"ForeignKey:NutrientID"`
	NutrientID    int64    `json:"nutno"`
	Food          Food     `gorm:"ForeignKey:FoodID"`
	FoodID        int64
}

// Database configuration
type Config struct {
	Db   string
	User string
	Pw   string
	Url  string
}
type DB struct {
	*gorm.DB
}
