package force

import (
	"errors"
	"fmt"
	"github.com/goforce/api/soap"
	"github.com/goforce/reloader/commons"
)

type Salesforce struct {
	Instances map[string]*Instance `json:"instances"`
	BatchSize int                  `json:"batchSize"`
}

type Instance struct {
	Url        string                 `json:"url"`
	Username   string                 `json:"username"`
	Password   string                 `json:"password"`
	Token      string                 `json:"token"`
	Values     map[string]interface{} `json:"values"`
	connector  func(instance *Instance) (*soap.Connection, error)
	connection *soap.Connection
}

type SalesforceSource struct {
	//	commons.EndPoint
	Instance string `json:"instance"`
	Query    string `json:"query"`
	SObject  string `json:"sObject"`
	instance *Instance
}

type SalesforceTarget struct {
	//	commons.EndPoint
	Instance   string `json:"instance"`
	SObject    string `json:"sobject"`
	Operation  string `json:"operation"`
	ExternalId string `json:"externalId"`
	BatchSize  int    `json:"batchSize"`
	Workers    int    `json:"workers"`
	instance   *Instance
	lookups    map[string]commons.Scan
}

// salesforce globals
var salesforce *Salesforce

func (config *Salesforce) Init(resolver func(string) string) error {
	salesforce = config
	if config == nil {
		return nil
	}
	for _, instance := range salesforce.Instances {
		instance.Url = resolver(instance.Url)
		instance.Username = resolver(instance.Username)
		instance.Password = resolver(instance.Password)
		instance.Token = resolver(instance.Token)
		for k, v := range instance.Values {
			if s, ok := v.(string); ok {
				instance.Values[k] = resolver(s)
			}
		}
		if instance.Values == nil {
			instance.Values = make(map[string]interface{})
		}
	}
	return nil
}

func (s *SalesforceSource) Init(resolver func(string) string) (err error) {
	if s == nil {
		return
	}
	// resolve names
	s.Instance = resolver(s.Instance)
	// validate
	if s.Instance == "" {
		return errors.New(fmt.Sprint("instance should be specified"))
	}
	if s.Query == "" && s.SObject == "" {
		return errors.New(fmt.Sprint("either query or sObject should be specified"))
	}
	if s.Query != "" && s.SObject != "" {
		return errors.New(fmt.Sprint("you should not set both query and sObject, only one allowed"))
	}
	// resolver instance
	s.instance, err = resolveInstance(s.Instance)
	return
}

func (s *SalesforceTarget) Init(resolver func(string) string) (err error) {
	if s == nil {
		return
	}
	// resolve names
	s.Instance = resolver(s.Instance)
	// validate
	if s.Instance == "" {
		return errors.New(fmt.Sprint("instance should be specified"))
	}
	if s.SObject == "" {
		return errors.New(fmt.Sprint("target sObject should be specified"))
	}
	if s.Operation == "" {
		return errors.New(fmt.Sprint("operation should be specified"))
	}
	// resolver instance
	s.instance, err = resolveInstance(s.Instance)
	// init lookups
	s.lookups = make(map[string]commons.Scan)
	return
}

func (s *SalesforceTarget) GetLabel() string {
	if s != nil {
		return s.Instance + "-" + s.SObject
	}
	return ""
}

func resolveInstance(name string) (*Instance, error) {
	if salesforce == nil {
		return nil, errors.New("configuration for salesforce should be provided")
	}
	if name != "" {
		if instance, ok := salesforce.Instances[name]; ok {
			return instance, nil
		} else {
			return nil, errors.New(fmt.Sprintf("no such instance:", name))
		}
	} else {
		return nil, nil
	}
}

// instance methods
func (ins *Instance) SetConnector(connector func(instance *Instance) (*soap.Connection, error)) {
	ins.connector = connector
}

func (ins *Instance) connect() (err error) {
	if ins.connection == nil {
		ins.connection, err = ins.connector(ins)
		return err
	}
	return nil
}
