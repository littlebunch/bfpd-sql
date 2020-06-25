# bfpd-sql
Simple command line utitly for loading Branded Food Products csv into a MySQL/MariaDB database.   
### How to run
1. Install [go v 13](https://golang.org/dl/) or greater and a recent version of [mariadb](https://mariadb.com)
2. Download the [branded foods and 'Supporting data for Downloads' csv](https://fdc.nal.usda.gov/download-datasets.html#bkmk-1) from Food Data Central and unzip into a directory of your choice.
3. Clone this repo into a directory of your choice and build the program: `go build -o bfpdloader main.go`
4. Create a schema: `mysql -u your-user -pyour-password -e'create schema bfpd'`
5. Install the schema using the one provided in the ./schema directory: `mysql -u your-user -pyour-password bfpd < bfpd-schema.sql`
6. In the build directory, create a config.yaml which matches the schema you just created:   
`url: 127.0.0.1`        
`db: bfpd`        
`pwd: your-password`       
`user: your-user`       
7. Load the nutrients table:  `./bfpdloader -t NUT -i /your-csv-install-path/nutrient.csv`
8. Load the derivations table: `./bfpdloader -t DERV -i /your-csv-install-path/food_nutrient_derivation.csv`
9. Load the branded foods database `./bfpdloader -t BFPD -i /your-csv-install-path/bfpd/`
