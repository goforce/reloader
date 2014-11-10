package force

import (
	"errors"
	"fmt"
	"github.com/goforce/api/commons"
	"strings"
)

func validateRecord(describe *commons.DescribeSObjectResult, record commons.Record) []error {
	errs := make([]error, 0)
NEXTFIELD:
	for _, f := range record.Fields() {
		fieldDescribe := describe.Get(f)
		if fieldDescribe == nil {
			continue
		}
		value, _ := record.Get(f)
		switch fieldDescribe.Type {
		case "picklist":
			vs, ok := value.(string)
			if !ok {
				errs = append(errs, errors.New(fmt.Sprint(f, ": picklist value not a string: ", value)))
				continue NEXTFIELD
			}
			if vs == "" {
				continue
			}
			for _, v := range fieldDescribe.PicklistValues {
				if strings.EqualFold(vs, v.Value) {
					if vs != v.Value {
						record.Set(f, v.Value)
					}
					continue NEXTFIELD
				}
			}
			errs = append(errs, errors.New(fmt.Sprint(f, ": missing picklist value: ", value)))
		}
	}
	return errs
}
