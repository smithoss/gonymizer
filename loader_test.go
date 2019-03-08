package gonymizer

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestLoaderTempDbCreation(t *testing.T) {
	conf := GetTestDbConf(TestDb + "_ANONYMIZER_LOADING")

	// Should come back with a DB created
	assert.Nil(t, CreateDatabase(conf))
	assert.Nil(t, DropDatabase(conf))
}

func TestLoadFile(t *testing.T) {
	_ = CloseTestDb(TestLoadFileDb)
	conf := GetTestDbConf(TestLoadFileDb)
	assert.Nil(t, CreateDatabase(conf))
	assert.Nil(t, CloseTestDb(TestLoadFileDb))
	assert.Nil(t, LoadFile(conf, TestProcessDumpfile))
}

func TestVerifyRowCount(t *testing.T) {
	conf := GetTestDbConf(TestDb)
	assert.Nil(t, VerifyRowCount(conf, TestRowCountFile))
	assert.Nil(t, VerifyRowCount(conf, TestRowCountIncorrectRowCountsFile)) // Should return Nil and print a warning
	assert.NotNil(t, VerifyRowCount(conf, TestRowCountsIncorrectNumberColumnsFile))
}
