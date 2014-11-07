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
	test      bool
	values    eval.Values
	functions eval.Functions
}

func (job *Job) Execute(globals *Globals) (err error) {

	startTime := time.Now()
	defer func() { log.Println(commons.PROGRESS, " job completed in ", time.Since(startTime)) }()
	log.Println(commons.PROGRESS, "starting job", job.Label)

	var reporter report.Reporter
	var sourceReader commons.Reader
	var targetWriter commons.Writer

	// defer closing of all in correct order
	defer func() {
		if targetWriter != nil {
			targetWriter.Close()
		}
		if sourceReader != nil {
			sourceReader.Close()
		}
		if reporter != nil {
			reporter.Close()
		}
	}()

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

	var targetFields = make([]string, 0, len(job.Rules))
	var aliases = make(map[string]*Rule)
	var skips = make([]*Rule, 0, len(job.Rules))
	var flags = make(map[string]map[string]bool)
	for _, rule := range job.Rules {
		if rule.Target != "" {
			targetFields = append(targetFields, rule.Target)
			flags[rule.Target] = rule.flags
		}
		if rule.Alias != "" {
			aliases[strings.ToUpper(rule.Alias)] = rule
		}
		if rule.Skip != "" {
			skips = append(skips, rule)
		}
	}

	sourceReader, err = source.NewReader()
	if err != nil {
		return errors.New(fmt.Sprint("error opening source reader in job ", job.Label, " :", err))
	}

	targetWriter, err = target.NewWriter(targetFields)
	if err != nil {
		return errors.New(fmt.Sprint("error opening target writer in job ", job.Label, " :", err))
	}
	if usesFlags, ok := targetWriter.(commons.UsesFlags); ok {
		usesFlags.SetFlags(flags)
	}
	targetWriter.SetTest(globals.test)

	valuesSupplier := target.NewValuesSupplier()
	targetFunctions := target.NewFunctionsSupplier()

	firsts := make(map[string]map[string]struct{})
	functionsSupplier := func(name string, args []interface{}) (interface{}, error) {
		switch name {
		case "FIRST":
			eval.NumOfParams(args, 2)
			if args[1] == nil {
				return true, nil
			}
			name := eval.MustBeString(args, 0)
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

	// create reporters
	defaultName := job.Label + time.Now().Format("-20060102150405")
	reporter = report.NewReporter(&job.Logs, defaultName, sourceReader.Fields(), targetWriter.Fields())

NEXTREC:
	for {
		sourceRecord, err := sourceReader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			return errors.New(fmt.Sprint("error reading source ", sourceReader.Location(), "\n", err))
		}

		report := reporter.NewReport(sourceRecord, sourceReader.Location())

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
		context.AddFunctions(func(name string, args []interface{}) (interface{}, error) {
			if name == "GET" {
				eval.NumOfParams(args, 1)
				s1 := eval.MustBeString(args, 0)
				if value, ok := sourceRecord.Get(s1); ok {
					return value, nil
				} else {
					panic(fmt.Sprint("no such source field: ", s1))
				}
			}
			return nil, eval.NOFUNC{}
		})
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
				panic("skip failed, not a bool")
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
				if _, err := targetRecord.Set(rule.Target, targetValue); err != nil {
					report.Error(err.Error())
					goto NEXTREC
				}
			}
		}
		err = targetWriter.Write(targetRecord, report, context)
		if err != nil {
			report.Error(fmt.Sprint("error writing target: ", err))
		}
	}
	err = targetWriter.Flush()
	if err != nil {
		return errors.New(fmt.Sprint("error flushing target: ", err))
	}
	return nil
}
