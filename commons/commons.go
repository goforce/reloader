package commons

import (
	"fmt"
	"github.com/goforce/eval"
	"strings"
)

var _ = fmt.Print

type Record interface {
	Get(string) (interface{}, bool)
	Set(string, interface{}) (interface{}, error)
	Fields() []string
}

type Report interface {
	Skip()
	Success(created bool, id string)
	Error(message string)
	Output(record Record)
}

type Source interface {
	NewReader() (Reader, error)
}

type Target interface {
	NewWriter([]string) (Writer, error)
	NewValuesSupplier() eval.Values
	NewFunctionsSupplier() eval.Functions
}

type Reader interface {
	Read() (Record, error)
	Location() string
	Close() error
}

type Writer interface {
	NewRecord() Record
	Write(Record, Report) error
	Flush() error
	Close() error
}

type ValueValidator func(string, interface{}) error

func GetAllFields(rec Record) []string {
	var flatten func([]string, Record)
	fields := make([]string, 0)
	flatten = func(prefixes []string, record Record) {
		for _, f := range record.Fields() {
			fullName := append(prefixes, f)
			n, _ := record.Get(f)
			if nested, ok := n.(Record); ok {
				flatten(fullName, nested)
			} else {
				fields = append(fields, strings.Join(fullName, "."))
			}
		}
	}
	flatten(make([]string, 0, 5), rec)
	return fields
}
