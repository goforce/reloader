package force

import (
	"errors"
	"fmt"
	"github.com/goforce/eval"
	"github.com/goforce/reloader/commons"
	"strings"
)

func (target *SalesforceTarget) NewValuesSupplier() eval.Values {
	return func(name string) (interface{}, bool) {
		if v, ok := target.instance.Values[name]; ok {
			return v, ok
		}
		return nil, false
	}
}

func (target *SalesforceTarget) NewFunctionsSupplier() eval.Functions {
	return func(name string, args []interface{}) (val interface{}, err error) {
		switch name {
		case "LOOKUP":
			// validate number of parameters
			var np int = (len(args) - 3) / 2
			if len(args) != np*2+3 {
				panic(fmt.Sprint("expected at least 3 parameters plus at least one pair to compare, actual: ", len(args)))
			}
			s1 := eval.MustBeString(args, len(args)-2)
			fields := make([]string, np)
			for i := 0; i < np; i++ {
				fields[i] = args[i*2+1].(string)
			}
			lookupName := fmt.Sprint(args[0], "/", strings.Join(fields, "/"))
			// was lookup created?
			scan, ok := target.lookups[lookupName]
			if !ok {
				// create lookup and read it
				source := &SalesforceSource{instance: target.instance, Query: args[0].(string)}
				scan, err = source.NewScan(&commons.Lookup{Keys: fields})
				if err != nil {
					panic(errors.New(fmt.Sprint("error creating lookup: ", err.Error())))
				}
				target.lookups[lookupName] = scan
			}
			var keys = make([]interface{}, np)
			for i := 0; i < np; i++ {
				keys[i] = args[i*2+2]
			}
			val, err = scan(keys, s1, args[len(args)-1])
			return val, err
		}
		return nil, eval.NOFUNC{}
	}
}
