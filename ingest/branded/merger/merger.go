package merger

import (
	"bufio"
	"log"
	"os"
	"strconv"
	"strings"
)

type line struct {
	id         int
	restOfLine string
}

/*func Merge(metadata string, branded string) {
	// read metadata
	metadataChan := make(chan *line)
	go reader(metadata, metadataChan)

	// read strength set IDs
	brandedChan := make(chan *line)
	go reader(branded, brandedChan)

	// join the two data streams
	mergedLinesChan := make(chan *line)
	go joiner(metadataChan, brandedChan, mergedLinesChan)

	for l := range mergedLinesChan {
		fmt.Printf("%v,%v\n", l.id, l.restOfLine)
	}
}*/
func Reader(fname string, out chan<- *line) {
	defer close(out) // close channel on return

	// open the file
	file, err := os.Open(fname)
	if err != nil {
		log.Fatalf("open: %v", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	header := true
	for scanner.Scan() {
		var l line
		columns := strings.SplitN(scanner.Text(), ",", 2)
		// ignore first line (header)
		if header {
			header = false
			continue
		}
		// convert ID to integer for easier comparison
		id, err := strconv.Atoi(strings.ReplaceAll(columns[0], "\"", ""))
		if err != nil {
			log.Fatalf("ParseInt: %v", err)
		}
		l.id = id
		l.restOfLine = columns[1]
		// send the line to the channel
		out <- &l
	}
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
}
func Joiner(metadata, setIDs <-chan *line, out chan<- *line) {
	defer close(out) // close channel on return

	bf := &line{}
	for md := range metadata {
		sep := ","
		// add matching branded_foods.csv line (if left over from previous iteration)
		if bf.id == md.id {
			md.restOfLine += sep + bf.restOfLine
			sep = " "
		}
		// look for matching branded foods
		for bf = range setIDs {
			// add all branded_foods.csv with matching IDs
			if bf.id == md.id {
				md.restOfLine += sep + si.restOfLine
				sep = " "
			} else if bf.id > md.id {
				break
			}
		}
		// send the augmented line into the channel
		out <- md
	}
}
