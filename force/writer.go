package force

import (
	"errors"
	"fmt"
	. "github.com/goforce/api/commons"
	"github.com/goforce/api/soap"
	"github.com/goforce/eval"
	"github.com/goforce/log"
	"github.com/goforce/reloader/commons"
	"strings"
)

const (
	MAX_BATCH_SIZE  int = 200
	MAX_NUM_WORKERS int = 5
)

type batchWork struct {
	records []Record
	reports []commons.Report
}

type ForceWriter struct {
	instance        *Instance
	sObjectDescribe *DescribeSObjectResult
	operation       string
	externalId      string
	batchSize       int
	batch           *batchWork
	workers         chan *batchWork
	nestedFields    map[string]*DescribeSObjectResult
	fields          []string
	test            bool
}

func (target *SalesforceTarget) NewWriter(fields []string) (commons.Writer, error) {
	if target == nil {
		return nil, nil
	}
	log.Println(commons.PROGRESS, fmt.Sprint("creating salesforce target: ", target.Instance))
	batchSize := target.BatchSize
	if batchSize == 0 {
		batchSize = salesforce.BatchSize
	}
	if batchSize <= 0 || batchSize > MAX_BATCH_SIZE {
		batchSize = MAX_BATCH_SIZE
	}
	numWorkers := target.Workers
	if numWorkers <= 0 {
		numWorkers = 1
	} else if numWorkers > MAX_NUM_WORKERS {
		numWorkers = MAX_NUM_WORKERS
	}
	writer := &ForceWriter{
		instance:     target.instance,
		operation:    strings.ToUpper(target.Operation),
		externalId:   target.ExternalId,
		batchSize:    batchSize,
		workers:      make(chan *batchWork, numWorkers),
		nestedFields: make(map[string]*DescribeSObjectResult),
		fields:       fields,
	}
	// init workers
	for i := 0; i < numWorkers; i++ {
		writer.workers <- &batchWork{records: make([]Record, 0, batchSize), reports: make([]commons.Report, 0, batchSize)}
	}
	err := target.instance.connect()
	if err != nil {
		return nil, errors.New(fmt.Sprint("not able to connect to target instance: ", target.Instance, "\n", err))
	}
	if target.SObject != "" {
		writer.sObjectDescribe, err = target.instance.connection.DescribeSObject(target.SObject)
		if err != nil {
			return nil, errors.New(fmt.Sprint("error describing SObject: ", target.SObject, "\n", err))
		}
		for _, f := range fields {
			fp := strings.Split(f, ".")
			if len(fp) > 1 {
				var fieldName string
				var referenceTo string
				ts := strings.Split(fp[0], ":")
				if len(ts) > 1 {
					fieldName = ts[0]
					referenceTo = ts[1]
				} else {
					fieldName = ts[0]
					if relfd := writer.sObjectDescribe.GetRelationship(fieldName); relfd == nil {
						return nil, errors.New(fmt.Sprint("no relationship: ", fieldName, " in: ", target.SObject))
					} else {
						if len(relfd.ReferenceTo) > 0 {
							referenceTo = relfd.ReferenceTo[0]
						} else {
							return nil, errors.New(fmt.Sprint("no relationship: ", fieldName, " in: ", target.SObject))
						}
					}
				}
				rede, err := target.instance.connection.DescribeSObject(referenceTo)
				if err != nil {
					return nil, errors.New(fmt.Sprint("error describing SObject: ", referenceTo, "\n", err))
				}
				writer.nestedFields[fieldName] = rede
			}
		}
	}
	return writer, nil
}

func (writer *ForceWriter) SetTest(test bool) {
	writer.test = test
}

func (writer *ForceWriter) Fields() []string {
	return writer.fields
}

func (writer *ForceWriter) NewRecord() commons.Record {
	r, err := NewDescribedRecord(writer.sObjectDescribe)
	for fieldName, describe := range writer.nestedFields {
		nested, err := NewDescribedRecord(describe)
		if err != nil {
			panic(err)
		}
		r.Set(fieldName, nested)
	}
	if err != nil {
		panic(err)
	}
	return r
}

func (writer *ForceWriter) Write(record commons.Record, report commons.Report, context eval.Context) error {
	// pull data for copy operation
	if writer.operation == "COPY" {

	}
	// validate all values
	errs := validateRecord(writer.sObjectDescribe, record.(Record))
	report.Output(record)
	if len(errs) > 0 {
		s := "reloader validation errors:\n"
		for _, err := range errs {
			s += "\n" + err.Error()
		}
		return errors.New(s)
	}
	if writer.batch == nil {
		writer.batch = <-writer.workers
	}
	if writer.test {
		report.Success(false, "")
	} else {
		writer.batch.records = append(writer.batch.records, record.(Record))
		writer.batch.reports = append(writer.batch.reports, report)
	}
	if len(writer.batch.records) == cap(writer.batch.records) {
		return writer.Flush()
	}
	return nil
}

func (writer *ForceWriter) Flush() error {
	if writer.batch == nil {
		return nil
	}
	batch := writer.batch
	writer.batch = nil
	go func() {
		if len(batch.records) > 0 {
			// write batch
			var results []soap.DmlResult
			var err error
			if writer.operation == "UPSERT" {
				results, err = writer.instance.connection.Upsert(batch.records, writer.externalId)
			} else if writer.operation == "UPDATE" {
				results, err = writer.instance.connection.Update(batch.records)
			} else if writer.operation == "INSERT" {
				results, err = writer.instance.connection.Insert(batch.records)
			} else if writer.operation == " COPY" {
				results, err = writer.instance.connection.Insert(batch.records)
			} else {
				panic(fmt.Sprint("unknown operation:", writer.operation))
			}
			if err != nil {
				log.Println(commons.ERRORS, err)
				panic("error calling salesforce api")
			}
			if len(batch.records) != len(results) {
				log.Println(commons.ERRORS, results)
				panic("incorrect result returned salesforce api")
			}
			for i, report := range batch.reports {
				result := results[i]
				if result.Success {
					report.Success(result.Created, result.Id)
				} else {
					report.Error(result.Errors.Message)
				}
			}
			// empty batch
			batch.records = make([]Record, 0, writer.batchSize)
			batch.reports = make([]commons.Report, 0, writer.batchSize)
		}
		// return worker
		writer.workers <- batch
	}()
	return nil
}

func (writer *ForceWriter) Close() error {
	if writer.workers == nil {
		return nil
	}
	writer.Flush()
	// wait all workers have finished
	for i := cap(writer.workers); i > 0; i-- {
		<-writer.workers
	}
	writer.workers = nil
	return nil
}
