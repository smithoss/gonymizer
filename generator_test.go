package gonymizer

import (
	"errors"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"os"
	"testing"
)

func TestCreateDumpFile(t *testing.T) {
	conf := GetTestDbConf(TestDb)

	assert.Nil(t,
		CreateDumpFile(
			conf,
			TestCreateFile,
			TestSchemaPrefix,
			TestExcludeTable,
			TestExcludeTableData,
			TestExcludeSchemas,
			TestSchemas,
			true, // Skip stored procedures for testing
		),
	)

	// Check dump file size
	size, err := ioutil.ReadFile(TestCreateFile)
	assert.Nil(t, err)
	if len(size) < 1500 {
		t.Fatalf("Expected file size to be > 1500 bytes. %s (%d)", TestCreateFile, size)
	}

	// Load the dump file to make sure it is a valid SQL file
	conf.DefaultDBName = TestPiiDb
	assert.Nil(t, DropDatabase(conf))
	assert.Nil(t, CreateDatabase(conf))
	assert.Nil(t, SQLCommandFile(conf, TestCreateFile, false))
}

func TestProcessDumpFile(t *testing.T) {
	conf := GetTestDbConf(TestDb)

	// Create a dump file from our test database
	assert.Nil(t,
		CreateDumpFile(
			conf,
			TestDumpFile,
			TestSchemaPrefix,
			TestExcludeTable,
			TestExcludeTableData,
			TestExcludeSchemas,
			TestSchemas,
			true,
		),
	)

	// Generate dump file
	columnMap, err := LoadConfigSkeleton(TestMapFile)
	assert.Nil(t, err)
	assert.Nil(t, ProcessDumpFile(columnMap, TestDumpFile, TestProcessDumpfile, "", true))

	// Load processed/anonymized dump file
	conf.DefaultDBName = TestPostLocalDb
	_ = DropDatabase(conf)
	assert.Nil(t, CreateDatabase(conf))
	assert.Nil(t, SQLCommandFile(conf, TestProcessDumpfile, false))
}

func TestGenerateRandomInt64(t *testing.T) {
	var test int64
	num, err := generateRandomInt64()
	assert.Nil(t, err)
	assert.IsType(t, test, num)
}

func TestGenerateSchemaSql(t *testing.T) {
	const testFileSize = 36

	conf := GetTestDbConf(TestDb)

	// Create file and put CREATE SCHEMA IF NOT EXISTS into these
	fp, err := os.OpenFile(TestGenerateSchemaFile, os.O_RDWR|os.O_CREATE, 0660)
	assert.Nil(t, err)
	assert.Nil(t, generateSchemaSQL(conf, fp, TestExcludeSchemas))
	assert.Nil(t, fp.Close())

	assert.Nil(t, VerifyFileSize(t, TestGenerateSchemaFile, testFileSize))
}

func TestClear(t *testing.T) {
	var line LineState

	line.IsRow = true
	line.SchemaName = "TestSchema"
	line.TableName = "TestTable"
	line.ColumnNames = []string{"TestColumnOne", "TestColumnTwo"}

	if line.IsRow == true || len(line.SchemaName) > 0 || len(line.TableName) > 0 || len(line.ColumnNames) > 0 {
		assert.Error(t, errors.New("LineState.Clear() did not clear the object!"))
	}
}

func TestPreProcess(t *testing.T) {
	const testFileSize = 311
	outFile, err := os.OpenFile(TestPreProcessFile, os.O_RDWR|os.O_CREATE, 0660)
	assert.Nil(t, err)

	err = preProcess(outFile)
	assert.Nil(t, err)
	assert.Nil(t, outFile.Close())

	assert.Nil(t, VerifyFileSize(t, TestPreProcessFile, testFileSize))
}

func TestPostProcess(t *testing.T) {
	const testFileSize = 590
	inFile, err := os.OpenFile(TestPreProcessFile, os.O_RDWR|os.O_APPEND, 0660)
	assert.Nil(t, err)

	assert.Nil(t, postProcess(inFile, TestPostProcessFile))
	assert.Nil(t, err)
	assert.Nil(t, inFile.Close())
	assert.Nil(t, VerifyFileSize(t, TestPreProcessFile, testFileSize))
}
