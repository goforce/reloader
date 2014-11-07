package main

import (
	"errors"
	"fmt"
	"github.com/goforce/eval"
	"github.com/goforce/reloader/commons"
	"github.com/goforce/reloader/csv"
	"github.com/goforce/reloader/force"
	"github.com/goforce/reloader/report"
	"strings"
)

type Config struct {
	Test       bool               `json:"test"`
	Salesforce *force.Salesforce  `json:"salesforce"`
	Csv        *csv.Csv           `json:"csv"`
	Lookups    map[string]*Lookup `json:"lookups"`
	Jobs       []*Job             `json:"jobs"`
	Logs       struct {
		Off   *bool  `json:"off"`
		Debug string `json:"debug"`
	} `json:"logs"`
}

type Lookup struct {
	commons.Lookup
	Source struct {
		Csv        *csv.CsvSource          `json:"csv"`
		Salesforce *force.SalesforceSource `json:"salesforce"`
	} `json:"source"`
}

type Job struct {
	Label  string `json:"label"`
	Source struct {
		Csv        *csv.CsvSource          `json:"csv"`
		Salesforce *force.SalesforceSource `json:"salesforce"`
	} `json:"source"`
	Target struct {
		Csv        *csv.CsvTarget          `json:"csv"`
		Salesforce *force.SalesforceTarget `json:"salesforce"`
	} `json:"target"`
	Rules []*Rule     `json:"rules"`
	Logs  report.Logs `json:"logs"`
}

type Rule struct {
	Source  string `json:"source"`
	Formula string `json:"formula"`
	Skip    string `json:"skip"`
	Alias   string `json:"alias"`
	Target  string `json:"target"`
	Omit    string `json:"omit"`
	Flags   string `json:"flags"`
	flags   map[string]bool
	formula eval.Expr
	skip    eval.Expr
	omit    eval.Expr
}

func (config *Config) ValidateAndCompileConfig(resolver func(string) string) []error {
	errs := make(listOfErrors, 0)
	// init connector configurations
	errs.add("salesforce config", config.Salesforce.Init(resolver))
	errs.add("csv config", config.Csv.Init(resolver))
	// init lookups
	for name, lookup := range config.Lookups {
		location := fmt.Sprint("lookup ", name)
		if lookup.Source.Salesforce == nil && lookup.Source.Csv == nil {
			errs.add(location, errors.New("source should be specified"))
		}
		if lookup.Source.Salesforce != nil && lookup.Source.Csv != nil {
			errs.add(location, errors.New("only one source can be specified"))
		}
		errs.add(location, lookup.Source.Salesforce.Init(resolver))
		errs.add(location, lookup.Source.Csv.Init(resolver))
	}
	// init jobs
	for i, job := range config.Jobs {
		location := fmt.Sprint("job #", i)
		// init source
		if job.Source.Salesforce == nil && job.Source.Csv == nil {
			errs.add(location, errors.New("source should be specified"))
		}
		if job.Source.Salesforce != nil && job.Source.Csv != nil {
			errs.add(location, errors.New("only one source can be specified"))
		}
		errs.add(location, job.Source.Salesforce.Init(resolver))
		errs.add(location, job.Source.Csv.Init(resolver))
		// init target
		if job.Target.Csv == nil && job.Target.Salesforce == nil {
			errs.add(location, errors.New("target should be specified"))
		}
		errs.add(location, job.Target.Salesforce.Init(resolver))
		errs.add(location, job.Target.Csv.Init(resolver))

		// parse rules and expressions
		aliases := make(map[string]bool)
		for _, m := range job.Rules {
			// check that no more than one source provided
			if m.Source != "" && m.Formula != "" {
				errs.add(fmt.Sprint("error in rule of job #", i), errors.New("either source or formula could be specified in one rule"))
			}
			// compile all expressions
			errs.add(fmt.Sprint("error in rule of job #", i), m.parseExpressionsAndFlags())
			// check that there are no duplicate aliasses
			alias := strings.ToUpper(m.Alias)
			if alias != "" && aliases[alias] {
				errs.add(fmt.Sprint("error in rule of job #", i), errors.New("same alias used more than once:"+alias))
			}
			aliases[alias] = true
		}
	}
	if len(errs) > 0 {
		return errs
	}
	return nil
}

func (config *Config) SetConfigDefaults() {
	for _, job := range config.Jobs {
		// job label will be used to default log names
		if job.Label == "" {
			if job.Target.Csv != nil {
				job.Label = job.Target.Csv.GetLabel()
			} else if job.Target.Salesforce != nil {
				job.Label = job.Target.Salesforce.GetLabel()
			}
		}
		// default logs
		var truebool bool = true
		var testbool bool = !config.Test
		job.Logs.Error.Off = onoff(job.Logs.Error.Off, job.Logs.Off, config.Logs.Off, nil)
		job.Logs.Success.Off = onoff(job.Logs.Success.Off, job.Logs.Off, config.Logs.Off, &truebool)
		job.Logs.Skip.Off = onoff(job.Logs.Skip.Off, job.Logs.Off, config.Logs.Off, nil)
		job.Logs.Output.Off = onoff(job.Logs.Output.Off, nil, nil, &testbool)
	}
}

func onoff(logOff *bool, jobOff *bool, topOff *bool, defOff *bool) *bool {
	if logOff != nil {
		return logOff
	} else if jobOff != nil {
		return jobOff
	} else if topOff != nil {
		return topOff
	} else {
		return defOff
	}
}

func (m *Rule) parseExpressionsAndFlags() (err error) {
	m.flags = make(map[string]bool)
	{
		for _, v := range strings.Split(m.Flags, ";") {
			m.flags[v] = true
		}
	}
	if m.Formula != "" {
		m.formula, err = eval.ParseString(m.Formula)
		if err != nil {
			return errors.New(fmt.Sprint(m.Formula, "\n", err))
		}
	}
	if m.Skip != "" {
		m.skip, err = eval.ParseString(m.Skip)
		if err != nil {
			return errors.New(fmt.Sprint(m.Skip, "\n", err))
		}
	}
	if m.Omit != "" {
		m.omit, err = eval.ParseString(m.Omit)
		if err != nil {
			return errors.New(fmt.Sprint(m.Omit, "\n", err))
		}
	}
	return nil
}

type listOfErrors []error

func (l *listOfErrors) add(location string, err error) {
	if err != nil {
		*l = append(*l, errors.New(fmt.Sprint("error in ", location, ": ", err)))
	}
}
