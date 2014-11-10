package csv

import (
	"encoding/csv"
	"errors"
	"fmt"
	data "github.com/goforce/api/commons"
	"github.com/goforce/eval"
	"github.com/goforce/reloader/commons"
	"io/ioutil"
	"os"
	"unicode/utf8"
)

type CsvWriter struct {
	filename string
	file     *os.File
	writer   *csv.Writer
	fields   []string
	test     bool
}

func (target *CsvTarget) NewWriter(fields []string) (commons.Writer, error) {
	var err error
	w := &CsvWriter{}
	if target.Path != nil {
		w.filename = *target.Path
		w.file, err = os.Create(*target.Path)
		if err != nil {
			return nil, errors.New(fmt.Sprint("cannot open file:", target.Path))
		}
		w.writer = csv.NewWriter(w.file)
	} else {
		w.filename = "NUL"
		w.writer = csv.NewWriter(ioutil.Discard)
	}
	if target.Comma != "" {
		c, _ := utf8.DecodeRuneInString(target.Comma)
		w.writer.Comma = c
	}
	w.fields = make([]string, len(fields))
	copy(w.fields, fields)
	return w, w.writer.Write(w.fields)
}

func (w *CsvWriter) Fields() []string {
	return w.fields
}

func (w *CsvWriter) SetTest(test bool) {
	w.test = test
}

func (w *CsvWriter) NewRecord() commons.Record {
	return &CsvRecord{fields: w.fields, values: make(map[string]string)}
}

func (w *CsvWriter) Write(record commons.Record, report commons.Report, context eval.Context) (err error) {
	row := make([]string, len(w.fields))
	for i, name := range w.fields {
		if value, ok := record.Get(name); ok {
			row[i] = data.String(value)
		}
	}
	if !w.test {
		err = w.writer.Write(row)
	}
	report.Output(record)
	if err == nil {
		report.Success(true, "")
	} else {
		report.Error(fmt.Sprint("error writing file: ", err))
	}
	return nil
}

func (w *CsvWriter) Flush() error {
	w.writer.Flush()
	return w.writer.Error()
}

func (w *CsvWriter) Close() error {
	w.writer.Flush()
	if err := w.writer.Error(); err != nil {
		return err
	}
	return w.file.Close()
}
