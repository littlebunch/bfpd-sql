// Package ds provides an interface for application calls to the datastore.
// To add a data source simply implement the methods
package ds

import "github.com/littlebunch/bfpd-sql/config"

// DataSource wraps the basic methods used for accessing and updating a
// data store.
type DataSource interface {
	ConnectDs(cs config.Config) error
	Get(q string, f interface{}) error
	Query(q string, f *[]interface{}) error
	GetDictionary(dt string, offset int32, max int32) ([]interface{}, error)
	Create(r []interface{}) error
	Remove(id string) error
	FoodExists(id string) bool
	CloseDs()
}
