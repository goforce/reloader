package report

import (
	"encoding/csv"
	"fmt"
	data "github.com/goforce/api/commons"
	"github.com/goforce/log"
	"github.com/goforce/reloader/commons"
	"io/ioutil"
	"os"
)

const (
	SUCCESS_LOG_CREATED string = "success__Created"
	SUCCESS_LOG_ID      string = "success__Id"
	ERROR_LOG_MESSAGE   string = "error__Message"
)

type Logs struct {
	Off     *bool  `json:"off"`
	Path    string `json:"path"`
	Error   Log    `json:"error"`
	Success Log    `json:"success"`
	Skip    Log    `json:"skip"`
	Output  Log    `json:"output"`
}

type Log struct {
	Off  *bool  `json:"off"`
	Path string `json:"path"`
}

type Reporter interface {
	Close()
	NewReport(commons.Record, string) *report
}

type reporter struct {
	skipWriter    *writer
	successWriter *writer
	errorWriter   *writer
	fields        []string
	outputWriter  *writer
	targetFields  []string
}

type report struct {
	reporter *reporter
	record   commons.Record
	reported bool
	location string
}

type writer struct {
	filename string
	initf    func()
	file     *os.File
	writer   *csv.Writer
}

func NewReporter(def *Logs, defaultPath string, fields []string, targetFields []string) *reporter {
	rr := reporter{}
	rr.fields = make([]string, len(fields))
	copy(rr.fields, fields)
	rr.targetFields = make([]string, len(targetFields))
	copy(rr.targetFields, targetFields)
	rr.skipWriter = newWriter(
		def.Skip,
		filename(def.Path, def.Skip.Path, defaultPath+"-skip.csv"),
		rr.fields)
	rr.successWriter = newWriter(
		def.Success,
		filename(def.Path, def.Success.Path, defaultPath+"-success.csv"),
		append(rr.fields, SUCCESS_LOG_CREATED, SUCCESS_LOG_ID))
	rr.errorWriter = newWriter(
		def.Error,
		filename(def.Path, def.Error.Path, defaultPath+"-error.csv"),
		append(rr.fields, ERROR_LOG_MESSAGE))
	rr.outputWriter = newWriter(
		def.Output,
		filename(def.Path, def.Output.Path, defaultPath+"-output.csv"),
		rr.targetFields)
	return &rr
}

func (rr *reporter) Close() {
	log.Println(commons.PROGRESS, "closing all report writers")
	rr.skipWriter.close()
	rr.successWriter.close()
	rr.errorWriter.close()
	rr.outputWriter.close()
}

func (rr *reporter) NewReport(record commons.Record, location string) *report {
	return &report{reporter: rr, record: record, reported: false, location: location}
}

func (r *report) Skip() {
	r.write(r.reporter.skipWriter)
}

func (r *report) Success(created bool, id string) {
	r.write(r.reporter.successWriter, fmt.Sprint(created), id)
}

// Error reports error to error log. If error log is off then error and location is printed using log topic reloader.errors
func (r *report) Error(message string) {
	if r.reporter.errorWriter.file == nil && r.reporter.errorWriter.initf == nil {
		log.Println(commons.ERRORS, "error at:", r.location, " / ", message)
	} else {
		r.write(r.reporter.errorWriter, message)
	}
}

func (r *report) Output(record commons.Record) {
	r.reporter.outputWriter.write(r.reporter.targetFields, record)
}

func (r *report) write(writer *writer, results ...string) {
	if r.reported {
		panic(fmt.Sprint("record reported more than once: ", r.record))
	}
	writer.write(r.reporter.fields, r.record, results...)
	r.reported = true
}

func filename(dir string, path string, defaultFilename string) string {
	if path == "" {
		path = defaultFilename
	}
	return dir + path
}

func newWriter(def Log, filename string, columns []string) *writer {
	w := &writer{filename: filename}
	if def.Off != nil && *def.Off {
		w.writer = csv.NewWriter(ioutil.Discard)
	} else {
		w.initf = func() {
			var err error
			w.file, err = os.Create(filename)
			if err != nil {
				panic(fmt.Sprint("error creating file:", w.filename, " : ", err))
			}
			w.writer = csv.NewWriter(w.file)
			err = w.writer.Write(columns)
			if err != nil {
				panic(fmt.Sprint("error writing log file:", w.filename, " : ", err))
			}
			w.initf = nil
		}
		if def.Off != nil {
			w.initf()
		}
	}
	return w
}

func (w *writer) write(fields []string, record commons.Record, results ...string) {
	if w.initf != nil {
		w.initf()
	}
	row := make([]string, 0, len(fields)+len(results))
	for _, name := range fields {
		if value, ok := record.Get(name); ok {
			row = append(row, data.String(value))
		} else {
			row = append(row, "")
		}
	}
	row = append(row, results...)
	err := w.writer.Write(row)
	if err != nil {
		panic(fmt.Sprint("error writing log file:", w.filename, " : ", err))
	}
}

func (w *writer) close() {
	if w != nil && w.writer != nil {
		w.writer.Flush()
		err := w.writer.Error()
		if err != nil {
			panic(fmt.Sprint("error writing log file:", w.filename, " : ", err))
		}
		if w.file != nil {
			err = w.file.Close()
			if err != nil {
				panic(fmt.Sprint("error closing log file:", w.filename, " : ", err))
			}
		}
	}
}
