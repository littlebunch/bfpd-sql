// Package bfpd provides types for the bfpd database model
package bfpd

import (
	"time"

	"github.com/jinzhu/gorm"
)

// Food reflects JSON used to transfer BFPD foods data from USDA csv
type Food struct {
	ID                 int32
	CreatedAt          time.Time
	UpdatedAt          time.Time
	DeletedAt          time.Time
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
	ID          int32
	CreatedAt   time.Time
	UpdatedAt   time.Time
	DeletedAt   time.Time
	Nutrientno  string `json:"nutno" binding:"required" gorm:"unique;not null"`
	Description string `json:"desc" binding:"required" gorm:"not null"`
	Unit        string
}
type Manufacturer struct {
	ID        int32
	CreatedAt time.Time
	UpdatedAt time.Time
	DeletedAt time.Time
	Version   uint8
	Name      string `json:"name" binding:"required"`
	Foods     []Food
}

type Unit struct {
	ID        int32
	CreatedAt time.Time
	UpdatedAt time.Time
	DeletedAt time.Time
	Version   uint8  `json:"version"`
	Unit      string `gorm:"unique;not null" json:"unit"`
	Nutrients []Nutrient
}
type SourceCode struct {
	CreatedAt    time.Time
	UpdatedAt    time.Time
	DeletedAt    time.Time
	ID           int32
	Code         string         `binding:"required" json:"code"`
	Description  string         `json:"desc"`
	NutrientData []NutrientData //`gorm:"ForeignKey:SourceID"`
}
type Derivation struct {
	ID           int32
	CreatedAt    time.Time
	UpdatedAt    time.Time
	DeletedAt    time.Time
	Code         string `binding:"required" json:"code"`
	Description  string `json:"desc"`
	NutrientData []NutrientData
}
type FoodGroup struct {
	ID          uint32
	CreatedAt   time.Time
	UpdatedAt   time.Time
	DeletedAt   time.Time
	Description string `json:"desc"`
	Food        []Food
}
type NutrientData struct {
	ID            int32
	CreatedAt     time.Time
	UpdatedAt     time.Time
	DeletedAt     time.Time
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
type DB struct {
	*gorm.DB
}
