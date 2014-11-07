package force

import (
	"fmt"
	. "github.com/goforce/api/commons"
	"github.com/goforce/log"
	"github.com/goforce/reloader/commons"
	"github.com/goforce/reloader/force/soqlparser"
)

type ForceReader struct {
	*QueryReader
	id     string
	fields []string
}

func (s *SalesforceSource) NewReader() (commons.Reader, error) {
	if s == nil {
		return nil, nil
	}
	log.Println(commons.PROGRESS, "querying solq: ", s.Query)
	if err := s.instance.connect(); err != nil {
		return nil, err
	}
	reader, err := NewReader(s.instance.connection.Query(s.Query))
	if err != nil {
		return nil, err
	}
	return &ForceReader{reader, "", soqlparser.SoqlFields(s.Query)}, nil
}

func (reader *ForceReader) Fields() []string {
	return reader.fields
}

func (reader *ForceReader) Read() (commons.Record, error) {
	if record, err := reader.QueryReader.Read(); err == nil {
		reader.id = fmt.Sprint(record.Get("Id"))
		return record, nil
	} else {
		return nil, err
	}
}

func (reader *ForceReader) Location() string {
	return fmt.Sprint("record Id: ", reader.id)
}

func (reader *ForceReader) Close() error {
	return nil
}
