package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/goforce/api/soap"
	"github.com/goforce/eval"
	"github.com/goforce/log"
	"github.com/goforce/reloader/commons"
	"github.com/goforce/reloader/force"
	"io/ioutil"
	"os"
	"time"
)

func main() {

	defer func() {
		if r := recover(); r != nil {
			fmt.Println("reloader failed with ERROR:", r)
		}
	}()

	config, errs := ReadConfigFile(os.Args[1], os.Args[2:])
	if len(errs) > 0 {
		for _, err := range errs {
			fmt.Println(err)
		}
		return
	}
	log.On(config.Logs.Debug)
	log.On(commons.PROGRESS)
	log.On(commons.ERRORS)
	log.Println(commons.PROGRESS, "config ok")

	// init strategy to connect to salesforce
	if config.Salesforce != nil {
		for _, instance := range config.Salesforce.Instances {
			instance.SetConnector(
				func(instance *force.Instance) (*soap.Connection, error) {
					return soap.Login(instance.Url, instance.Username, instance.Password+instance.Token)
				})
		}
	}

	// read in all lookups
	globalScans := make(map[string]commons.Scan)
	for name, lkp := range config.Lookups {
		fmt.Println("creating lookup:", name)
		var source commons.LookupSource
		if lkp.Source.Salesforce != nil {
			source = lkp.Source.Salesforce
		} else if lkp.Source.Csv != nil {
			source = lkp.Source.Csv
		}
		scan, err := source.NewScan(&lkp.Lookup)
		if err != nil {
			log.Println(commons.ERRORS, err)
			return
		}
		globalScans[name] = scan
	}
	globals := &Globals{test: config.Test}
	globals.functions = func(name string, args []interface{}) (val interface{}, err error) {
		switch name {
		case "SCAN":
			s1 := eval.MustBeString(args, 0, "SCAN")
			s2 := eval.MustBeString(args, len(args)-2, "SCAN")
			scan, ok := globalScans[s1]
			if !ok {
				return nil, errors.New(fmt.Sprint("function SCAN: lookup not found:", s1))
			}
			if len(args) < 3 {
				return nil, errors.New(fmt.Sprint("function SCAN: at least 3 arguments expected, actual:", len(args)))
			}
			keys := args[1 : len(args)-2]
			val, err = scan(keys, s2, args[len(args)-1])
			return val, err
		}
		return nil, eval.NOFUNC{}
	}

	// execute all jobs
	for _, job := range config.Jobs {
		err := job.Execute(globals)
		if err != nil {
			fmt.Println(err)
			return
		}
	}

}

func ReadConfigFile(filename string, args []string) (*Config, []error) {
	configInput, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, []error{errors.New(fmt.Sprint("cannot open config file: ", filename, "\n", err))}
	}
	// remove ctrl chars used for readability
	configInput = bytes.Replace(configInput, []byte{'\n'}, []byte{' '}, -1)
	configInput = bytes.Replace(configInput, []byte{'\r'}, []byte{' '}, -1)
	configInput = bytes.Replace(configInput, []byte{'\t'}, []byte{' '}, -1)
	// parse config file
	config := &Config{}
	err = json.Unmarshal(configInput, config)
	if err != nil {
		return nil, []error{errors.New(fmt.Sprint("error parsing config file: ", filename, "\n", err))}
	}
	// make parameters for name resolving
	var params [111]interface{}
	for i, v := range args {
		params[i] = v
	}
	// add time dependent parameters
	now := time.Now()
	params[100] = fmt.Sprint(now.Year())
	params[101] = fmt.Sprint(now.Month())
	params[102] = fmt.Sprint(now.Day())
	params[103] = fmt.Sprint(now.Hour())
	params[104] = fmt.Sprint(now.Minute())
	params[105] = fmt.Sprint(now.Second())
	params[110] = ""
	resolver := func(name string) string {
		return fmt.Sprintf(name+"%[111]s", params[0:]...)
	}
	return config, config.ValidateAndCompileConfig(resolver)
}
