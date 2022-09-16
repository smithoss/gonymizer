package gonymizer

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

var emptyList = []string{}

const no_oids_flag = false
const oids_flag_present = true

func TestMinimalDumpArgs(t *testing.T) {
	conf := GetDbConf()
	args := CreateDumpArgs(
		conf,
		TestCreateFile,
		TestSchemaPrefix,
		emptyList,
		emptyList,
		emptyList,
		emptyList,
		no_oids_flag,
	)
	expected_args := []string{
		"--no-owner",
		"-f", "testing/output.TestCreateFile.sql",
		"postgres://user:password@test_host/db_name?sslmode=disable",
	}

	assert.Equal(t, expected_args, args, "they should be equal")
}

func TestCanProvideSchemas(t *testing.T) {
	conf := GetDbConf()
	args := CreateDumpArgs(
		conf,
		TestCreateFile,
		TestSchemaPrefix,
		emptyList,
		emptyList,
		emptyList,
		[]string{"schema1", "schema2"},
		no_oids_flag,
	)
	expected_args := []string{
		"--no-owner",
		"--schema=schema1",
		"--schema=schema2",
		"-f", "testing/output.TestCreateFile.sql",
		"postgres://user:password@test_host/db_name?sslmode=disable",
	}

	assert.Equal(t, expected_args, args, "they should be equal")
}

func TestSchemaPrefixAddsWildcard(t *testing.T) {
	conf := GetDbConf()
	args := CreateDumpArgs(
		conf,
		TestCreateFile,
		"company_",
		emptyList,
		emptyList,
		emptyList,
		[]string{"company"},
		no_oids_flag,
	)
	expected_args := []string{
		"--no-owner",
		"--schema=company_*",
		"-f", "testing/output.TestCreateFile.sql",
		"postgres://user:password@test_host/db_name?sslmode=disable",
	}

	assert.Equal(t, expected_args, args, "they should be equal")
}

func TestCanExcludeSchemas(t *testing.T) {
	conf := GetDbConf()
	args := CreateDumpArgs(
		conf,
		TestCreateFile,
		TestSchemaPrefix,
		emptyList,
		emptyList,
		[]string{"bad-schema1", "bad-schema2", "bad-schema3"},
		emptyList,
		no_oids_flag,
	)
	expected_args := []string{
		"--no-owner",
		"--exclude-schema=bad-schema1",
		"--exclude-schema=bad-schema2",
		"--exclude-schema=bad-schema3",
		"-f", "testing/output.TestCreateFile.sql",
		"postgres://user:password@test_host/db_name?sslmode=disable",
	}

	assert.Equal(t, expected_args, args, "they should be equal")
}

func TestCanExcludeTables(t *testing.T) {
	conf := GetDbConf()
	args := CreateDumpArgs(
		conf,
		TestCreateFile,
		TestSchemaPrefix,
		[]string{"bad-table1", "bad-table2"},
		emptyList,
		emptyList,
		emptyList,
		no_oids_flag,
	)
	expected_args := []string{
		"--no-owner",
		"--exclude-table=bad-table1",
		"--exclude-table=bad-table2",
		"-f", "testing/output.TestCreateFile.sql",
		"postgres://user:password@test_host/db_name?sslmode=disable",
	}

	assert.Equal(t, expected_args, args, "they should be equal")
}

func TestCanExcludeTableData(t *testing.T) {
	conf := GetDbConf()
	args := CreateDumpArgs(
		conf,
		TestCreateFile,
		TestSchemaPrefix,
		emptyList,
		[]string{"bad-table1", "bad-table2"},
		emptyList,
		emptyList,
		no_oids_flag,
	)
	expected_args := []string{
		"--no-owner",
		"--exclude-table-data=bad-table1",
		"--exclude-table-data=bad-table2",
		"-f", "testing/output.TestCreateFile.sql",
		"postgres://user:password@test_host/db_name?sslmode=disable",
	}

	assert.Equal(t, expected_args, args, "they should be equal")
}

func TestCanAddOIDSFlag(t *testing.T) {
	conf := GetDbConf()
	args := CreateDumpArgs(
		conf,
		TestCreateFile,
		TestSchemaPrefix,
		emptyList,
		emptyList,
		emptyList,
		emptyList,
		oids_flag_present,
	)
	expected_args := []string{
		"--no-owner",
		"--oids",
		"-f", "testing/output.TestCreateFile.sql",
		"postgres://user:password@test_host/db_name?sslmode=disable",
	}

	assert.Equal(t, expected_args, args, "they should be equal")
}

func GetDbConf() PGConfig {
	conf := PGConfig{}
	conf.Username = "user"
	conf.Pass = "password"
	conf.Host = "test_host"
	conf.DefaultDBName = "db_name"
	conf.SSLMode = "disable"
	return conf
}
