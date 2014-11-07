package csv

import (
	"encoding/csv"
	"errors"
	"fmt"
	"github.com/goforce/log"
	"github.com/goforce/reloader/commons"
	"io"
	"os"
	"unicode/utf8"
)

type CsvReader struct {
	filename string
	file     *os.File
	reader   *csv.Reader
	fields   []string
	linenum  uint
}

func (c *CsvSource) NewReader() (commons.Reader, error) {
	log.Println(commons.PROGRESS, "opening csv reader: ", c.Path)
	r := &CsvReader{filename: c.Path, linenum: 0}
	var err error
	r.file, err = os.Open(c.Path)
	if err != nil {
		return nil, errors.New(fmt.Sprint("cannot open file:", c.Path))
	}
	r.reader = csv.NewReader(r.file)
	if c.Comma != "" {
		c, _ := utf8.DecodeRuneInString(c.Comma)
		r.reader.Comma = c
	}
	r.reader.LazyQuotes = c.LazyQuotes
	// read in first row with field names
	r.fields, err = r.reader.Read()
	if err == io.EOF {
		r.fields = make([]string, 0)
		return r, nil
	} else if err != nil {
		return nil, err
	}
	return r, nil
}

func (r *CsvReader) Fields() []string {
	if r == nil {
		return make([]string, 0)
	}
	return r.fields
}

func (r *CsvReader) Read() (commons.Record, error) {
	values, err := r.reader.Read()
	if err != nil {
		return nil, err
	}
	r.linenum++
	rec := &CsvRecord{fields: r.fields, values: make(map[string]string)}
	for i, v := range values {
		rec.values[r.fields[i]] = v
	}
	return rec, nil
}

func (r *CsvReader) Location() string {
	return fmt.Sprint("line number: ", r.linenum)
}

func (r *CsvReader) Close() error {
	if r.file != nil {
		return r.file.Close()
	}
	return nil
}
