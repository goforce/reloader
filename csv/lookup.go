package csv

import (
	"errors"
	"fmt"
	"github.com/goforce/log"
	"github.com/goforce/reloader/commons"
	"io"
)

func (source *CsvSource) NewScan(lookup *commons.Lookup) (commons.Scan, error) {
	log.Println(commons.PROGRESS, "initializing lookup: ", source.Path)
	reader, err := source.NewReader()
	if reader != nil {
		defer reader.Close()
	}
	records := make([]commons.Record, 0, 100)
	for {
		rec, err := reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, errors.New(fmt.Sprint("error reading source ", reader.Location(), "\n", err))
		}
		records = append(records, rec)
	}
	scan, err := lookup.GetScan(records)
	return scan, err
}
