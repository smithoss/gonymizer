package gonymizer

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestLoaderTempDbCreation(t *testing.T) {
	conf := GetTestDbConf(TestDb + "_ANONYMIZER_LOADING")

	// Should come back with a DB created
	require.Nil(t, CreateDatabase(conf))
	require.Nil(t, DropDatabase(conf))
}

func TestLoadFile(t *testing.T) {
	_ = CloseTestDb(TestLoadFileDb)
	conf := GetTestDbConf(TestLoadFileDb)
	require.Nil(t, CreateDatabase(conf))
	require.Nil(t, LoadFile(conf, TestProcessDumpfile))
	require.Nil(t, CloseTestDb(TestLoadFileDb))
}

func TestVerifyRowCount(t *testing.T) {
	conf := GetTestDbConf(TestDb)
	require.Nil(t, VerifyRowCount(conf, TestRowCountFile))
	require.Nil(t, VerifyRowCount(conf, TestRowCountIncorrectRowCountsFile)) // Should return Nil and print a warning
	require.NotNil(t, VerifyRowCount(conf, TestRowCountsIncorrectNumberColumnsFile))
}
