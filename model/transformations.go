package bfpd

//"fmt"
type TransformedFood struct {
	ID           int64                     `json:"id"`
	Ndbno        string                    `json:"ndbno"`
	Description  string                    `json:"desc"`
	Manufacturer string                    `json:"manu"`
	Ingredients  string                    `json:"ingred"`
	FoodGroup    TransformedFoodGroup      `json:"group"`
	Source       string                    `json:"source"`
	Nutrients    []TransformedNutrientData `json:"nutrients"`
}
type TransformedNutrient struct {
	Nutrientno  string `json:"nutno"`
	Description string `json:"name"`
}
type TransformedFoodGroup struct {
	Code        string `json:"code"`
	Description string `json:"desc"`
}
type TransformedIngredients struct {
	Description  string `json:"desc" binding:"required"`
	Available    string `json:"avail"`        //	The date the data for the food item represented by the specific GTIN was made available on the market.
	Discontinued string `json:"discontinued"` //	The data indicated by the manufacturer that the product represented by a specific GTIN has been discontinued
	Updated      string `json:"updated"`
}
type TransformedNutrientData struct {
	Nutrient       TransformedNutrient `json:"nutrient"`
	Value          float32             `json:"value"`
	Datapoints     uint32              `json:"dp"`
	StandardError  float32             `json:"se"`
	AddNutMark     string              `json:"nutmark"`
	NumberStudies  uint8               `json:"studies"`
	Minimum        float32             `json:"min"`
	Maximum        float32             `json:"max"`
	DegreesFreedom float32             `json:"df"`
	LowerEB        float32             `json:"lb"`
	UpperEB        float32             `json:"ub"`
	Comment        string              `json:"comment"`
	ConfidenceCode string              `json:"code"`
	Source         TransformedCodes    `json:"source"`
	Derivation     TransformedCodes    `json:"derv"`
}
type TransformedCodes struct {
	Code        string
	Description string
}

func (t *TransformedNutrientData) Transform(nd *[]NutrientData) []TransformedNutrientData {
	var b []TransformedNutrientData
	for _, n := range *nd {
		b = append(b, TransformedNutrientData{
			Nutrient: TransformedNutrient{Nutrientno: n.Nutrient.Nutrientno,
				Description: n.Nutrient.Description},
			//Source:        TransformedCodes{Code: n.Sourcecode.Code, Description: n.Sourcecode.Description},
			Derivation:    TransformedCodes{Code: n.Derivation.Code, Description: n.Derivation.Description},
			Value:         n.Value,
			Datapoints:    n.Datapoints,
			StandardError: n.StandardError,
			NumberStudies: n.NumberStudies})
	}
	return b
}
