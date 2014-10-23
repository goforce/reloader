package force

import (
	"github.com/goforce/reloader/commons"
)

func (c *SalesforceTarget) GetValueValidator() commons.ValueValidator {
	return func(name string, value interface{}) error { return nil }
}

//func validateValue(describe *force.DescribeSObjectResult, name string, value interface{}) error {
//	fieldDescribe, ok := describe.FieldsMap[name]
//	if !ok {
//		return nil
//	}
//	switch fieldDescribe.Type {
//	case "picklist":
//		vs, ok := value.(string)
//		if !ok {
//			return errors.New(fmt.Sprint(name, ": picklist value not string: ", value))
//		}
//		if vs == "" {
//			return nil
//		}
//		for _, v := range fieldDescribe.PicklistValues {
//			if vs == v.Value {
//				return nil
//			}
//		}
//		return errors.New(fmt.Sprint(name, ": missing picklist value: ", value))
//	}
//	return nil
//}
