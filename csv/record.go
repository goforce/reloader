package csv

import (
	"github.com/goforce/api/commons"
	"strings"
)

type CsvRecord struct {
	fields []string
	values map[string]string
}

func (rec CsvRecord) Get(name string) (interface{}, bool) {
	if k, ok := rec.findName(name); ok {
		return rec.values[k], true
	}
	return nil, false
}

func (rec CsvRecord) Set(name string, value interface{}) (interface{}, error) {
	if k, ok := rec.findName(name); ok {
		rec.values[k] = commons.String(value)
	} else {
		rec.values[name] = commons.String(value)
	}
	return value, nil
}

func (rec CsvRecord) Fields() []string {
	return rec.fields
}

func (rec CsvRecord) findName(name string) (string, bool) {
	_, ok := rec.values[name]
	if ok {
		return name, true
	}
	for k, _ := range rec.values {
		if ok := strings.EqualFold(k, name); ok {
			return k, true
		}
	}
	return "", false
}
