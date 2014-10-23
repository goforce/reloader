package main

import (
	"errors"
	"fmt"
	types "github.com/goforce/api/commons"
	"github.com/goforce/eval"
	"github.com/goforce/log"
	"github.com/goforce/reloader/commons"
	"github.com/goforce/reloader/report"
	"io"
	"strings"
	"time"
)

type Globals struct {
	values    eval.Values
	functions eval.Functions
}

func (job *Job) Execute(globals *Globals) (err error) {

	startTime := time.Now()
	defer func() { log.Println(commons.PROGRESS, " job completed in ", time.Since(startTime)) }()
	log.Println(commons.PROGRESS, "starting job", job.Label)

	var source commons.Source
	if job.Source.Salesforce != nil {
		source = job.Source.Salesforce
	} else if job.Source.Csv != nil {
		source = job.Source.Csv
	}
	var target commons.Target
	if job.Target.Salesforce != nil {
		target = job.Target.Salesforce
	} else if job.Target.Csv != nil {
		target = job.Target.Csv
	}

	var targetFields []string = make([]string, 0, len(job.Rules))
	var aliases map[string]*Rule = make(map[string]*Rule)
	var skips []*Rule = make([]*Rule, 0, len(job.Rules))
	for _, rule := range job.Rules {
		if rule.Target != "" {
			targetFields = append(targetFields, rule.Target)
		}
		if rule.Alias != "" {
			aliases[strings.ToUpper(rule.Alias)] = rule
		}
		if rule.Skip != "" {
			skips = append(skips, rule)
		}
	}

	sourceReader, err := source.NewReader()
	if sourceReader != nil {
		defer sourceReader.Close()
	}
	if err != nil {
		return errors.New(fmt.Sprint("error opening source reader in job ", job.Label, " :", err))
	}

	targetWriter, err := target.NewWriter(targetFields)
	if targetWriter != nil {
		defer targetWriter.Close()
	}
	if err != nil {
		return errors.New(fmt.Sprint("error opening target writer in job ", job.Label, " :", err))
	}

	valuesSupplier := target.NewValuesSupplier()
	targetFunctions := target.NewFunctionsSupplier()

	firsts := make(map[string]map[string]struct{})
	functionsSupplier := func(name string, args []interface{}) (interface{}, error) {
		switch strings.ToUpper(name) {
		case "FIRST":
			if len(args) != 2 {
				return nil, errors.New(fmt.Sprint("function FIRST expected 2 parameters, actual: ", len(args)))
			}
			if args[1] == nil {
				return true, nil
			}
			name, ok := args[0].(string)
			if !ok {
				return nil, errors.New(fmt.Sprint("function FIRST expects first parameter to be string, actual: ", args[0]))
			}
			f, ok := firsts[name]
			if !ok {
				f = make(map[string]struct{})
				f[types.String(args[1])] = struct{}{}
				firsts[name] = f
				return true, nil
			}
			if _, ok := f[types.String(args[1])]; ok {
				return false, nil
			}
			f[types.String(args[1])] = struct{}{}
			return true, nil
		}
		return nil, eval.NOFUNC{}
	}

	var firstRun bool = true
	var reporter report.Reporter

NEXTREC:
	for {
		sourceRecord, err := sourceReader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			return errors.New(fmt.Sprint("error reading source ", sourceReader.Location(), "\n", err))
		}

		if firstRun {
			// create reporters
			defaultName := job.Label + time.Now().Format("-20060102150405")
			reporter = report.NewReporter(&job.Logs, defaultName, commons.GetAllFields(sourceRecord))
			defer reporter.Close()
			firstRun = false
		}
		report := reporter.NewReport(sourceRecord)

		// create name resolving function used in expressions
		context := eval.NewContext()
		context.AddValues(func(name string) (interface{}, bool) {
			if value, ok := sourceRecord.Get(name); ok {
				return value, true
			}
			return nil, false
		})
		context.AddValues(func(name string) (interface{}, bool) {
			if rule, ok := aliases[strings.ToUpper(name)]; ok {
				if rule.Source != "" {
					if value, ok := sourceRecord.Get(rule.Source); ok {
						return value, true
					}
				} else if rule.formula != nil {
					v, err := rule.formula.Eval(context)
					if err != nil {
						panic(err)
					}
					return v, true
				} else {
					panic("only source or formula can be aliased...")
				}
			}
			return nil, false
		})
		context.AddValues(valuesSupplier)
		context.AddValues(globals.values)
		context.AddFunctions(functionsSupplier)
		context.AddFunctions(targetFunctions)
		context.AddFunctions(globals.functions)

		// handle all skips
		for _, skip := range skips {
			v, err := skip.skip.Eval(context)
			if err != nil {
				panic(fmt.Sprint("error evaluating skip expression: ", err))
			}
			switch v.(type) {
			case bool:
				if v.(bool) {
					report.Skip()
					goto NEXTREC
				}
			default:
				//TODO make fail to be skip and write to skip file
				panic("skip failed, cannot cast to bool")
			}
		}

		targetRecord := targetWriter.NewRecord()

		for _, rule := range job.Rules {
			var targetValue interface{}
			var ok bool
			if rule.Source != "" {
				targetValue, ok = sourceRecord.Get(rule.Source)
				if !ok {
					panic(fmt.Sprint("no such field in source:", rule.Source))
				}
			} else if rule.formula != nil {
				targetValue, err = rule.formula.Eval(context)
				if err != nil {
					report.Error(fmt.Sprint("error in formula: ", err))
					goto NEXTREC
				}
			}
			omit := false
			if rule.omit != nil {
				o, err := rule.omit.Eval(context)
				if err != nil {
					report.Error(fmt.Sprint("error in omit: ", err))
					goto NEXTREC
				}
				if omit, ok = o.(bool); !ok {
					report.Error(fmt.Sprint("omit should evaluate to boolean", o))
					goto NEXTREC
				}
			}
			if rule.Target != "" && !omit {
				//						if !m.rules["novalidation"] {
				//							if err := validateValue(descr, m.Target, rec[m.Source]); err != nil {
				//								fmt.Println(sourceLine, err)
				//							}
				//						}
				if _, err := targetRecord.Set(rule.Target, targetValue); err != nil {
					report.Error(err.Error())
					goto NEXTREC
				}
			}
		}
		err = targetWriter.Write(targetRecord, report)
		if err != nil {
			report.Error(fmt.Sprint("error in formula: ", err))
		}
	}
	err = targetWriter.Flush()
	if err != nil {
		return errors.New(fmt.Sprint("error flushing target: ", err))
	}
	return nil
}
