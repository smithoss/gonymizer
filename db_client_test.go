package gonymizer

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCheckIfDbExists(t *testing.T) {
	conf := GetTestDbConf(TestDb)

	// First connect to postgres db to get connection
	conf.DefaultDBName = "postgres"
	dbConn, err := OpenDB(conf)
	assert.Nil(t, err)
	assert.NotNil(t, dbConn)

	// Next check to make sure the database exists
	doesExist, err := CheckIfDbExists(dbConn, conf.DefaultDBName)
	assert.Nil(t, err)
	assert.True(t, doesExist)
}

func TestGetAllProceduresInSchema(t *testing.T) {
	conf := GetTestDbConf(TestDb)

	// Check to make sure Public schema has procedures
	procs, err := GetAllProceduresInSchema(conf, "public")
	if len(procs) < 1 {
		t.Fatal("Using 'public' as our schema we received 0 procedures back")
	}
	assert.Nil(t, err)
}

func TestGetAllSchemaColumns(t *testing.T) {
	conf := GetTestDbConf(TestDb)
	dbConn, err := OpenDB(conf)
	assert.Nil(t, err)
	assert.NotNil(t, dbConn)
	_, err = GetAllSchemaColumns(dbConn)
	assert.Nil(t, err)
}

func TestGetAllTablesInSchema(t *testing.T) {
	conf := GetTestDbConf(TestDb)

	// Check to make sure empty string input for schema works
	tables, err := GetAllTablesInSchema(conf, "")
	if len(tables) < 1 {
		t.Fatal("Using empty string as our schema we received 0 tables back")
	}
	assert.Nil(t, err)

	// Check to make sure public schema has tables
	tables, err = GetAllTablesInSchema(conf, "public")
	if len(tables) < 1 {
		t.Fatal("Using empty string '' as our schema we received 0 tables back")
	}
	assert.Nil(t, err)

}

func TestGetSchemasInDatabase(t *testing.T) {
	conf := GetTestDbConf(TestDb)
	_, err := GetSchemasInDatabase(conf, []string{"public"})
	assert.Nil(t, err)
	_, err = GetSchemasInDatabase(conf, []string{})
	assert.Nil(t, err)
}

func TestGetSchemaColumnEquals(t *testing.T) {
	conf := GetTestDbConf(TestDb)
	dbConn, err := OpenDB(conf)
	assert.Nil(t, err)

	// Get column data for "public" schema
	_, err = GetSchemaColumnEquals(dbConn, "public")
	assert.Nil(t, err)
}

func TestGetTableRowCountsInDB(t *testing.T) {
	conf := GetTestDbConf(TestDb)
	counts, err := GetTableRowCountsInDB(conf, "", nil)
	assert.Nil(t, err)

	total := 0
	for _, row := range *counts {
		total += *row.Count
	}
	if total < 1 {
		t.Fatal("Unable to get any row counts")
	}
}

func TestRenameDatabase(t *testing.T) {
	var count int

	const tempDbFrom = "anon_rename_db_test"
	const tempDbTo = tempDbFrom + "_renamed"

	conf := GetTestDbConf(tempDbTo)

	// Make sure to remove previous failures
	_ = DropDatabase(conf)
	conf = GetTestDbConf(tempDbFrom)
	_ = DropDatabase(conf)

	assert.Nil(t, CreateDatabase(conf))

	// Rename database
	conf.DefaultDBName = "postgres" // Switch to postgres so we are not connected to DBs to be renamed
	dbConn, err := OpenDB(conf)
	assert.Nil(t, err)
	assert.Nil(t, RenameDatabase(dbConn, tempDbFrom, tempDbTo))

	query := `
		SELECT COUNT(*)
		FROM pg_catalog.pg_database
		WHERE datname=$1
	`

	// Check to see if it exist
	result := dbConn.QueryRow(query, tempDbTo)
	err = result.Scan(&count)
	assert.Nil(t, err)
	if count < 1 {
		t.Fatalf("Renamed database '%s'->'%s', but could not find the latter in pg_catalog", tempDbFrom, tempDbTo)
	}
	conf.DefaultDBName = tempDbTo
	assert.Nil(t, DropDatabase(conf))
}
