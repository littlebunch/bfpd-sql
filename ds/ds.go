// Package ds provides an interface for application calls to the datastore.
// To add a data source simply implement the methods
package ds

import (
	bfpd "github.com/littlebunch/bfpd-sql/model"
)

// DataSource wraps the basic methods used for accessing and updating a
// data store.
type DataSource interface {
	ConnectDs(cs bfpd.Config) error
	Get(q string, f interface{}) error
	Query(q string, f *[]interface{}) error
	Update(id string, r interface{}) error
	Remove(id string) error
	FoodExists(id string) bool
	InitDb()
	CloseDs()
}
