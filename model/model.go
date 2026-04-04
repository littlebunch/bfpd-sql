// Package bfpd provides types for the bfpd database model
package bfpd

import (
	"time"

	"gorm.io/gorm"
)

// Food reflects JSON used to transfer BFPD foods data from USDA csv
type Food struct {
	gorm.Model
	PublicationDate time.Time      `json:"publicationDateTime"`
	ModifiedDate    time.Time      `json:"modifiedDate,omitempty"`
	AvailableDate   time.Time      `json:"availableDate,omitempty"`
	DiscontinueDate time.Time      `json:"discontinueDate,omitempty"`
	Upc             string         `json:"upc" binding:"required" gorm:"unique;not null"`
	FdcID           string         `json:"fdcId" binding:"required"`
	Description     string         `json:"name" binding:"required"`
	FoodGroup       FoodGroup      `gorm:"foreignKey:FoodGroupID" json:"fg"`
	FoodGroupID     int64          `json:"food_group_id"`
	Ingredients     string         `json:"ingredients"`
	Brand           Brand          `json:"brand" gorm:"foreignKey:BrandID"`
	BrandID         int64          `json:"brand_id"`
	Datasource      string         `json:"source"`
	NutrientData    []NutrientData `json:"nutrients" gorm:"foreignKey:FoodID"`
	Serving         Serving        `json:"serving" gorm:"foreignKey:ServingID"`
	ServingID       int64          `json:"serving_id"`
	Country         string         `json:"marketCountry,omitempty"`
}

type Nutrient struct {
	gorm.Model
	Nutrientno  string `json:"nutno" binding:"required" gorm:"unique;not null"`
	Description string `json:"desc" binding:"required" gorm:"not null"`
	Unit        string
}
type Brand struct {
	gorm.Model
	Version  uint8
	Brand    string `json:"brand"`
	SubBrand string `json:"subBrand"`
	Owner    string `json:"owner" binding:"required" gorm:"unique; not null"`
	Foods    []Food
}
type Serving struct {
	gorm.Model
	Description string `json:"desc"`
	Size        float32
	Unit        string
	Foods       []Food
}
type SourceCode struct {
	gorm.Model
	Code         string         `binding:"required" json:"code"`
	Description  string         `json:"desc"`
	NutrientData []NutrientData `gorm:"foreignKey:SourceID" json:"nutrients"`
}
type Derivation struct {
	gorm.Model
	Code         string `binding:"required" json:"code"`
	Description  string `json:"desc"`
	NutrientData []NutrientData
}
type FoodGroup struct {
	gorm.Model
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
	Derivation    Derivation `json:"deriv" gorm:"foreignKey:DerivationID"`
	DerivationID  int64
	SourceCode    SourceCode `json:"src" gorm:"foreignKey:SourceID"`
	SourceID      int64
	Nutrient      Nutrient `gorm:"foreignKey:NutrientID"`
	NutrientID    int64    `json:"nutno"`
	Food          Food     `gorm:"foreignKey:FoodID"`
	FoodID        int64
}
