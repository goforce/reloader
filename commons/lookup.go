package commons

import (
	"errors"
	"fmt"
)

type LookupSource interface {
	NewScan(*Lookup) (Scan, error)
}

type Lookup struct {
	Keys  []string `json:"keys"`
	Value string   `json:"value"`
}

type Scan func([]interface{}, interface{}) (interface{}, error)

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
					if nv[kv], ok = rec.Get(lookup.Value); !ok {
						return nil, errors.New(fmt.Sprint("no field found:", key))
					}
				} else {
					nm := make(lookupData)
					nv[kv] = nm
					nv = nm
				}
			}
		}
	}
	return func(keys []interface{}, defaultValue interface{}) (interface{}, error) {
		if len(keys) != len(lookup.Keys) {
			// TODO lookup name or solq+keys (for unnamed lookups) should be shown
			return nil, errors.New(fmt.Sprintf("incorrect number of key values passed for lookup", lookup))
		}
		nv := data
		for i := 0; i < len(keys); i++ {
			if _, ok := nv[keys[i]]; ok {
				if i == len(keys)-1 {
					return nv[keys[i]], nil
				} else {
					if nv, ok = nv[keys[i]].(lookupData); !ok {
						return nil, errors.New("internal error")
					}
				}
			} else {
				return defaultValue, nil
			}
		}
		return nil, errors.New("internal error")
	}, nil
}
