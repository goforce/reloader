package csv

import (
	"errors"
	"fmt"
	"github.com/goforce/eval"
	"path/filepath"
	"strings"
)

type Csv struct{}

type CsvSource struct {
	Path       string `json:"path"`
	Comma      string `json:"comma"`
	LazyQuotes bool   `json:"lazyQuotes"`
}

type CsvTarget struct {
	Path  *string `json:"path"`
	Comma string  `json:"comma"`
}

func (config *Csv) Init(resolver func(string) string) error {
	return nil
}

func (c *CsvSource) Init(resolver func(string) string) (err error) {
	if c == nil {
		return
	}
	// resolve names
	c.Path = resolver(c.Path)
	// validate
	if c.Path == "" {
		return errors.New(fmt.Sprint("path should be specified for csv source"))
	}
	return nil
}

func (c *CsvTarget) Init(resolver func(string) string) (err error) {
	if c == nil {
		return
	}
	// resolve names
	if c.Path != nil {
		path := resolver(*c.Path)
		// validate
		if path == "" {
			return errors.New(fmt.Sprint("path should be specified for csv target"))
		}
		c.Path = &path
	}
	return nil
}

func (c *CsvTarget) GetLabel() string {
	if c != nil {
		if c.Path != nil {
			label := filepath.Base(*c.Path)
			return strings.TrimSuffix(label, filepath.Ext(label))
		} else {
			return "discarded"
		}
	}
	return ""
}

func (c *CsvTarget) NewValuesSupplier() eval.Values {
	return nil
}

func (c *CsvTarget) NewFunctionsSupplier() eval.Functions {
	return nil
}
