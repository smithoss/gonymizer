package gonymizer

import (
	"errors"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCreateDumpFile(t *testing.T) {
	conf := GetTestDbConf(TestDb)

	require.Nil(t,
		CreateDumpFile(
			conf,
			TestCreateFile,
			TestSchemaPrefix,
			TestExcludeTable,
			TestExcludeTableData,
			TestExcludeSchemas,
			TestSchemas,
		),
	)

	// Check dump file size
	size, err := ioutil.ReadFile(TestCreateFile)
	require.Nil(t, err)
	if len(size) < 1500 {
		t.Fatalf("Expected file size to be > 1500 bytes. %s (%d)", TestCreateFile, size)
	}

	// Load the dump file to make sure it is a valid SQL file
	conf.DefaultDBName = TestPiiDb
	require.Nil(t, DropDatabase(conf))
	require.Nil(t, CreateDatabase(conf))
	require.Nil(t, SQLCommandFile(conf, TestCreateFile, true))
}

func TestProcessDumpFile(t *testing.T) {
	conf := GetTestDbConf(TestDb)

	// Create a dump file from our test database
	require.Nil(t,
		CreateDumpFile(
			conf,
			TestDumpFile,
			TestSchemaPrefix,
			TestExcludeTable,
			TestExcludeTableData,
			TestExcludeSchemas,
			TestSchemas,
		),
	)

	// Generate dump file
	columnMap, err := LoadConfigSkeleton(TestMapFile)
	require.Nil(t, err)
	require.Nil(t, ProcessDumpFile(columnMap, TestDumpFile, TestProcessDumpfile, "", true))

	// Load processed/anonymized dump file
	conf.DefaultDBName = TestPostLocalDb
	_ = DropDatabase(conf)
	require.Nil(t, CreateDatabase(conf))
	require.Nil(t, SQLCommandFile(conf, TestProcessDumpfile, true)) //Must ignore errors
}

func TestGenerateRandomInt64(t *testing.T) {
	var test int64
	num, err := generateRandomInt64()
	require.Nil(t, err)
	require.IsType(t, test, num)
}

func TestGenerateSchemaSql(t *testing.T) {

	conf := GetTestDbConf(TestDb)

	// Create file and put CREATE SCHEMA IF NOT EXISTS into these
	fp, err := os.OpenFile(TestGenerateSchemaFile, os.O_RDWR|os.O_CREATE, 0660)
	require.Nil(t, err)
	require.Nil(t, generateSchemaSQL(conf, fp, TestExcludeSchemas))
	require.Nil(t, fp.Close())

}

func TestClear(t *testing.T) {
	var line LineState

	line.IsRow = true
	line.SchemaName = "TestSchema"
	line.TableName = "TestTable"
	line.ColumnNames = []string{"TestColumnOne", "TestColumnTwo"}

	if line.IsRow == true || len(line.SchemaName) > 0 || len(line.TableName) > 0 || len(line.ColumnNames) > 0 {
		require.Error(t, errors.New("LineState.Clear() did not clear the object because of invalid line"))
	}
}

func TestPreProcess(t *testing.T) {
	outFile, err := os.OpenFile(TestPreProcessFile, os.O_RDWR|os.O_CREATE, 0660)
	require.Nil(t, err)

	err = preProcess(outFile)
	require.Nil(t, err)
	require.Nil(t, outFile.Close())

}

func TestPostProcess(t *testing.T) {
	inFile, err := os.OpenFile(TestPreProcessFile, os.O_RDWR|os.O_APPEND, 0660)
	require.Nil(t, err)

	require.Nil(t, postProcess(inFile, TestPostProcessFile))
	require.Nil(t, err)
	require.Nil(t, inFile.Close())
}
