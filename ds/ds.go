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
	Create(r []interface{}) error
	Remove(id int64) error
	FoodExists(id string) bool
	CloseDs()
}
