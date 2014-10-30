package commons

import (
	"errors"
	"fmt"
)

type LookupSource interface {
	NewScan(*Lookup) (Scan, error)
}

type Lookup struct {
	Keys []string `json:"keys"`
}

type Scan func([]interface{}, string, interface{}) (interface{}, error)

var GlobalLookups = make(map[string]Scan)

func (lookup *Lookup) GetScan(records []Record) (Scan, error) {
	type lookupData map[interface{}]interface{}
	data := make(lookupData)
	for _, rec := range records {
		nv := data
		for i, key := range lookup.Keys {
			kv, ok := rec.Get(key)
			if !ok {
				return nil, errors.New(fmt.Sprint("no field found:", key))
			}
			//				if _, ok := kv.(string); ok {
			//					kv = strings.ToUpper(kv.(string))
			//				}
			lastkey := i+1 == len(lookup.Keys)
			v, ok := nv[kv]
			if ok {
				if lastkey {
					return nil, errors.New(fmt.Sprint("duplicate value in key:", kv, " : ", v))
				} else {
					nv = v.(lookupData)
				}
			} else {
				if lastkey {
					nv[kv] = rec
				} else {
					nm := make(lookupData)
					nv[kv] = nm
					nv = nm
				}
			}
		}
	}
	return func(keys []interface{}, valueField string, defaultValue interface{}) (interface{}, error) {
		if len(keys) != len(lookup.Keys) {
			// TODO lookup name or solq+keys (for unnamed lookups) should be shown
			return nil, errors.New(fmt.Sprintf("incorrect number of key values passed for lookup", lookup))
		}
		nv := data
		for i := 0; i < len(keys); i++ {
			if _, ok := nv[keys[i]]; ok {
				if i == len(keys)-1 {
					if rec, ok := nv[keys[i]].(Record); ok {
						if v, ok := rec.Get(valueField); ok {
							return v, nil
						}
						return nil, errors.New(fmt.Sprint("no field found:", valueField))
					}
					return nil, errors.New("internal error: not a record")
				} else {
					if nv, ok = nv[keys[i]].(lookupData); !ok {
						return nil, errors.New("internal error: not a data")
					}
				}
			} else {
				return defaultValue, nil
			}
		}
		return nil, errors.New("internal error: fatal")
	}, nil
}
