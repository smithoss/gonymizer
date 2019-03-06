package gonymizer

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGenerateConfigSkeleton(t *testing.T) {
	conf := GetTestDbConf(TestDb)

	// Dump testing database
	excludeTables := append(TestExcludeTable, TestExcludeTableData...)
	skeleton, err := GenerateConfigSkeleton(conf, TestSchemaPrefix, TestSchemas, excludeTables)
	assert.Nil(t, err)

	_, err = GenerateConfigSkeleton(conf, TestSchemaPrefix, []string{}, excludeTables)
	assert.Nil(t, err)
	_, err = GenerateConfigSkeleton(conf, TestSchemaPrefix, TestSchemas, []string{})
	assert.Nil(t, err)

	err = WriteConfigSkeleton(skeleton, TestMapOutputFile)
	assert.Nil(t, err)
	err = WriteConfigSkeleton(skeleton, "")
	assert.NotNil(t, err)

}

func TestLoadConfigSkeleton(t *testing.T) {
	dbmap, err := LoadConfigSkeleton(TestMapFile)
	assert.Nil(t, err)
	assert.Nil(t, dbmap.Validate())
	dbmap, err = LoadConfigSkeleton(TestDbFile)
	assert.NotNil(t, err)
	dbmap, err = LoadConfigSkeleton("")
	assert.NotNil(t, err)
	dbmap, err = LoadConfigSkeleton("/dev/null")
	assert.NotNil(t, err)
}
