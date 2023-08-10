package configs

import (
	"fmt"
	"io/ioutil"

	"gopkg.in/yaml.v3"
)

const (
	defaultConfigFile = "config.yaml"
)

// Conf holds all the necessary information for running the comparison.
type Conf struct {
	Database1          *Database       `yaml:"database"`
	Database2          *Database       `yaml:"database2"`
	Dir                string          `yaml:"dir"`
	Dir2               string          `yaml:"dir2"`
	IgnoreTables       []string        `yaml:"ignore_tables"`
	IgnoreColumns      []string        `yaml:"ignore_columns"`
	IgnoreTableColumns []*TableColumns `yaml:"ignore_table_columns"`
	IgnoreTypes        []string        `yaml:"ignore_types"`
	Limit              int             `yaml:"limit"`
	Detailed           bool            `yaml:"detailed"`

	// These fields are handled when reading the config file and will be used
	// to know wich tables, columns and types are to be ignored during comparison.
	ignoreTypeMap        map[string]bool
	ignoreTableMap       map[string]bool
	ignoreColumnMap      map[string]bool
	ignoreTableColumnMap map[string]map[string]bool
}

type Database struct {
	Label    string `yaml:"label"`
	Host     string `yaml:"host"`
	Port     string `yaml:"port"`
	Database string `yaml:"database"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
}

type TableColumns struct {
	TableName string   `yaml:"table_name"`
	Columns   []string `yaml:"columns"`
}

// GetConf returns the config struct from the given yaml file.
// GetConf will also handle the tables, columns and types to be ignored, populating
// the correspondent fields.
func GetConf(configFile string) (*Conf, error) {
	// Check if given config file is empty. If yes, read from defaultConfigFile.
	if configFile == "" {
		configFile = defaultConfigFile
	}

	// Read the file
	yamlFile, err := ioutil.ReadFile(configFile)
	if err != nil {
		return nil, fmt.Errorf("reading config file \"%s\": %v ", configFile, err)
	}

	// Unmarshal file bytes into struct
	c := &Conf{}
	err = yaml.Unmarshal(yamlFile, c)
	if err != nil {
		return nil, fmt.Errorf("unmarshaling config file: %v", err)
	}

	// Handle the tables, columns and types to be ignored
	c.ignoreTableMap = make(map[string]bool)
	for _, t := range c.IgnoreTables {
		c.ignoreTableMap[t] = true
	}

	c.ignoreColumnMap = make(map[string]bool)
	for _, t := range c.IgnoreColumns {
		c.ignoreColumnMap[t] = true
	}

	c.ignoreTableColumnMap = make(map[string]map[string]bool)
	for _, t := range c.IgnoreTableColumns {
		c.ignoreTableColumnMap[t.TableName] = make(map[string]bool)
		for _, col := range t.Columns {
			c.ignoreTableColumnMap[t.TableName][col] = true
		}
	}

	c.ignoreTypeMap = make(map[string]bool)
	for _, t := range c.IgnoreTypes {
		c.ignoreTypeMap[t] = true
	}

	return c, nil
}

// IsTableToBeIgnored returns if given table is to be ignored.
func (c Conf) IsTableToBeIgnored(table string) bool {
	return c.ignoreTableMap[table]
}

// IsColumnToBeIgnored returns if column is to be ignored according to given table and column.
//
// Column will be ignored if given column is present in c.IgnoreColumns or if
// given table and column are present in c.IgnoreTableColumns.
func (c Conf) IsColumnToBeIgnored(table, column string) bool {
	return c.ignoreColumnMap[column] || c.ignoreTableColumnMap[table][column]
}

func (c Conf) IsTypeToBeIgnored(t string) bool {
	return c.ignoreTypeMap[t]
}
