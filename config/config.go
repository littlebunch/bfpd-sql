package config

import (
	"io/ioutil"
	"os"

	yaml "gopkg.in/yaml.v2"
)

//Config provides basic configuration properties for API services.  Properties are normally read in from a YAML file or the environment
//Each datastore should have it's own type
type Config struct {
	Db   string
	User string
	Pwd  string
	URL  string
}

// Defaults sets values for CouchBase configuration properties if none have been provided.
func (cs *Config) Defaults() {
	if os.Getenv("DB_URL") != "" {
		cs.URL = os.Getenv("DB_URL")
	}

	if os.Getenv("DB_USER") != "" {
		cs.User = os.Getenv("DB_USER")
	}
	if os.Getenv("DB_PWD") != "" {
		cs.Pwd = os.Getenv("DB_PWD")
	}
	if cs.URL == "" {
		cs.URL = "localhost"
	}
}

// GetConfig reads config from a file
func (cs *Config) GetConfig(c *string) error {
	raw, err := ioutil.ReadFile(*c)
	if err == nil {
		if err = yaml.Unmarshal(raw, cs); err == nil {
			cs.Defaults()
		}
	}
	return err
}
