package gonymizer

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestGenerateConfigSkeleton(t *testing.T) {
	conf := GetTestDbConf(TestDb)

	// Dump testing database
	excludeTables := append(TestExcludeTable, TestExcludeTableData...)
	skeleton, err := GenerateConfigSkeleton(conf, TestSchemaPrefix, TestSchemas, excludeTables)
	require.Nil(t, err)

	_, err = GenerateConfigSkeleton(conf, TestSchemaPrefix, []string{}, excludeTables)
	require.Nil(t, err)
	_, err = GenerateConfigSkeleton(conf, TestSchemaPrefix, TestSchemas, []string{})
	require.Nil(t, err)

	err = WriteConfigSkeleton(skeleton, TestMapOutputFile)
	require.Nil(t, err)
	err = WriteConfigSkeleton(skeleton, "")
	require.NotNil(t, err)

}

func TestLoadConfigSkeleton(t *testing.T) {
	dbmap, err := LoadConfigSkeleton(TestMapFile)
	require.Nil(t, err)
	require.Nil(t, dbmap.Validate())
	_, err = LoadConfigSkeleton(TestDbFile)
	require.NotNil(t, err)
	_, err = LoadConfigSkeleton("")
	require.NotNil(t, err)
	_, err = LoadConfigSkeleton("/dev/null")
	require.NotNil(t, err)
}
