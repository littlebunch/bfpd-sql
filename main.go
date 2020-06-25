package main

/*
* Command line utility to bulk load csv representation of bpfd foods.  Uses json decoder, prepared statements and transactions.
* Often requires tweaking innodb_lock_wait_timeout to avoid concurrency deadlock on transaction blocks,e.g > 1000 .
* The default timeout value is rather high -- 5000.
 */
import (
	"flag"
	"log"
	"os"
	"sync"

	"github.com/littlebunch/bfpd-sql/config"
	"github.com/littlebunch/bfpd-sql/ds/mariadb"
	"github.com/littlebunch/bfpd-sql/ingest"
	"github.com/littlebunch/bfpd-sql/ingest/branded"
	"github.com/littlebunch/bfpd-sql/ingest/dictionaries"
	bfpd "github.com/littlebunch/bfpd-sql/model"
)

var wg sync.WaitGroup
var tokens = make(chan struct{}, 200)
var a = flag.Bool("a", false, "Create schema")
var i = flag.String("i", "", "Input JSON file name")
var c = flag.String("c", "config.yaml", "YAML config file")
var t = flag.String("t", "BFPD", "Type of data to ingest: BFPD, DERV, GROUP, NUT or NUTDATA")
var n = flag.Int("n", 5000, "Number of foods in a transaction")

func main() {
	var (
		cs  config.Config
		in  ingest.Ingest
		dt  bfpd.DocType
		mdb mariadb.SqlDb
	)
	flag.Parse()
	if *i == "" {
		log.Fatal("CSV input path is required ")
	}
	dtype := dt.ToDocType(*t)
	if dtype == 999 {
		log.Fatalln("Valid t option is required ", *t)
	}

	err := cs.GetConfig(c)
	if err != nil {
		log.Fatal("Cannot process config: ", err.Error())
	}
	ds := &mdb
	err = ds.ConnectDs(cs)
	defer ds.CloseDs()
	if err != nil {
		log.Fatal("failed to connect to database ", err.Error())
	}
	db := ds.Conn
	db.SetMaxIdleConns(10)
	db.SetMaxOpenConns(300)
	// implement the Ingest interface
	if dtype == bfpd.BFPD {
		in = branded.Bfpd{Doctype: dt.ToString(bfpd.BFPD)}
	} else {
		in = dictionaries.Dictionary{Dt: dtype}
	}
	// ingest the csv files
	if err := in.ProcessFiles(*i, ds); err != nil {
		log.Fatal(err)
	}

	log.Println("Finished.")
	os.Exit(0)
}
