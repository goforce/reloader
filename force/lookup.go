package force

import (
	. "github.com/goforce/api/commons"
	"github.com/goforce/reloader/commons"
)

func (source *SalesforceSource) NewScan(lookup *commons.Lookup) (commons.Scan, error) {
	if err := source.instance.connect(); err != nil {
		return nil, err
	}
	records, err := ReadAll(source.instance.connection.Query(source.Query))
	if err != nil {
		return nil, err
	}
	converted := make([]commons.Record, len(records))
	for i, v := range records {
		converted[i] = commons.Record(v)
	}
	scan, err := lookup.GetScan(converted)
	return scan, err
}
