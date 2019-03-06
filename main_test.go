package gonymizer

import (
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

/*
	NOTICE: This is the main file for driving all testing
*/

// Test Databases
const TestDb = "anon_test_db"
const TestPiiDb = "anon_generator_test_pii"
const TestPostLocalDb = "anon_generator_test_postprocess"
const TestLoadFileDb = "anon_loader_test"

// Input Test Files
const TestDbFile = "testing/test_db.sql"
const TestMapFile = "testing/test_map.json"
const TestPostProcessFile = "testing/test_post_process.sql"
const TestRowCountFile = "testing/test_row_counts.csv"
const TestRowCountIncorrectRowCountsFile = "testing/test_row_counts_incorrect_row_counts.csv"
const TestRowCountsIncorrectNumberColumnsFile = "testing/test_row_counts_incorrect_number_columns.csv"
// Output test files
const TestCreateFile = "testing/output.TestCreateFile.sql"
const TestDumpFile = "testing/output.TestDumpFile.sql"
const TestGenerateSchemaFile = "testing/output.TestGenerateSchemaFile.sql"
const TestMapOutputFile = "testing/output.TestMapperFile.json"
const TestPreProcessFile = "testing/output.TestPreProcessFile.sql"
const TestProcessDumpfile = "testing/output.TestProcessDumpFile.sql"

// Test schemaPrefix
const TestSchemaPrefix = ""


// Test tables, exclude tables, and schemas
var (
	TestExcludeTable     = []string{"distributors"}
	TestExcludeTableData = []string{"purchasers"}

	// Test Schemas and exclude creating system Schemas
	TestSchemas        = []string{"public"}
	TestExcludeSchemas = []string{
		"pg_toast",
		"pg_temp_1",
		"pg_toast_temp_1",
		"pg_catalog",
		"information_schema",
	}
	TestColumnMapper = []DBMapper{}
)

// GetTestDbConf will return a PGConfig for the specified localhost testing database.
func GetTestDbConf(dbName string) PGConfig {
	conf := PGConfig{}
	conf.Host = "localhost"
	conf.DefaultDBName = dbName
	conf.SSLMode = "disable"
	return conf
}

// LoadTestDb will load the test database into the supplied database name on localhost.
func LoadTestDb(dbName string) error {
	conf := GetTestDbConf(dbName)

	// Prep database
	if err := DropDatabase(conf); err != nil {
	}
	if err := CreateDatabase(conf); err != nil {
		return err
	}

	// Load testing database
	if err := SQLCommandFile(conf, TestDbFile, false); err != nil {
		return err
	}
	return nil
}

// CloseTestDb will drop the supplied test database from localhost. This is useful when using: defer CloseTestDb
// after completing a test in which the database is loaded.
func CloseTestDb(dbName string) error {
	conf := GetTestDbConf(dbName)
	if err := DropDatabase(conf); err != nil {
		return err
	}
	return nil
}

// TestStart is the main entry point for all tests. Testing should be started using the Go test -run TestStart
// command.
func TestStart(t *testing.T) {
	// Set logrus to FATAL only so we do not see output from application
	logrus.SetLevel(logrus.FatalLevel)
	removeTestFiles(t)
	seqUnitTests(t)
}

/********************************************************************************************************************
Quick way to build the below statements for a given test file. Can do the same for lib_name_test.go by simply echoing
different input variables.

TEST_FILE="file_to_test.go"
for i in $(grep func $TEST_FILE |awk '{print $2}' | cut -d '(' -f 1); do
	echo "t.Run(\"$(echo $i | sed 's/Test//g')\", $i)"
done

********************************************************************************************************************/

// SeqUnitTests is where we keep our unit tests that require sequential runs in-order
// For example: CREATE/DROP DB should follow each other
// NOTE: ORDER MATTERS HERE FOR TESTS
func seqUnitTests(t *testing.T) {
	t.Run("DSN", TestDSN)
	t.Run("BaseDSN", TestBaseDSN)
	t.Run("URI", TestURI)
	t.Run("BaseURI", TestBaseURI)
	t.Run("OpenDB", TestOpenDB)
	t.Run("PsqlCmd", TestPsqlCommand)

	// Processors.go
	t.Run("ProcessorFunc", TestProcessorFunc)
	t.Run("ProcessorAlphaNumericScrambler", TestProcessorAlphaNumericScrambler)
	t.Run("ProcessorAddress", TestProcessorAddress)
	t.Run("ProcessorCity", TestProcessorCity)
	t.Run("ProcessorEmailAddress", TestProcessorEmailAddress)
	t.Run("ProcessorFirstName", TestProcessorFirstName)
	t.Run("ProcessorFakeFullName", TestProcessorFullName)
	t.Run("ProcessorIdentity", TestProcessorIdentity)
	t.Run("ProcessorLastName", TestProcessorLastName)
	t.Run("ProcessorPhoneNumber", TestProcessorPhoneNumber)
	t.Run("ProcessorState", TestProcessorState)
	t.Run("ProcessorUserName", TestProcessorUserName)
	t.Run("ProcessorZip", TestProcessorZip)
	t.Run("ProcessorRandomDate", TestProcessorRandomDate)
	t.Run("ProcessorRandomUUID", TestProcessorRandomUUID)
	t.Run("ProcessorScrubString", TestProcessorScrubString)
	t.Run("randomizeUUID", TestRandomizeUUID)

	// Below are tests that require the test database to be loaded into Postgres for testing functionality. This requires
	// one to update the map file as well as create fake data in the testing/test_db.sql file when  updating users

	// db_client.go - test and remove testing database
	t.Run("CreateDatabase", TestCreateDatabase)
	t.Run("DropDatabase", TestDropDatabase)
	t.Run("DropDatabase (IF EXISTS)", TestDropDatabase) // DROP IF NOT EXISTS should ignore missing DB

	assert.Nil(t, LoadTestDb(TestDb))
	t.Run("CreateDumpFile", TestCreateDumpFile)

	// Load test database for testing sequential tests
	t.Run("PsqlCommand", TestPsqlCommand)

	// db_client.go
	t.Run("CheckIfDbExists", TestCheckIfDbExists)
	t.Run("GetAllProceduresInSchema", TestGetAllProceduresInSchema)
	t.Run("GetTableRowCountsInDB", TestGetTableRowCountsInDB)
	t.Run("GetAllSchemaColumns", TestGetAllSchemaColumns)
	t.Run("GetAllTablesInSchema", TestGetAllTablesInSchema)
	t.Run("GetSchemasInDatabase", TestGetSchemasInDatabase)
	t.Run("GetSchemaColumnEquals", TestGetSchemaColumnEquals)
	t.Run("RenameDatabase", TestRenameDatabase)

	// mapper.go
	t.Run("LoadConfigSkeleton", TestLoadConfigSkeleton)
	t.Run("GenerateConfigSkeleton", TestGenerateConfigSkeleton)

	// Generate.go
	t.Run("GenerateRandomInt64", TestGenerateRandomInt64)
	t.Run("GenerateSchemaSql", TestGenerateSchemaSql)
	t.Run("PreProcess", TestPreProcess)
	t.Run("ProcessDumpFile", TestProcessDumpFile)
	t.Run("PostProcess", TestPostProcess)
	t.Run("Clear", TestClear)

	// Test loader.go
	t.Run("LoadFile", TestLoadFile)
	t.Run("TempDbCreate", TestLoaderTempDbCreation)
	t.Run("VerifyRowCounts", TestVerifyRowCount)

	// db_client.go / DB Cleanup
	t.Run("DropDatabase", TestDropDatabase)
	t.Run("DropDatabase (IF EXISTS)", TestDropDatabase) // DROP IF NOT EXISTS should ignore missing DB

	// Version.go
	t.Run("Version", TestVersion)
	t.Run("BuildNumber", TestBuildNumber)
	t.Run("BuildDate", TestBuildDate)

}


// removeTestFiles removes temporary test files that may be hanging around from the testing directory before
// running another round. Because of the appending nature of some tests this could cause false positives when
// running integration tests.
func removeTestFiles(t *testing.T) {
	var (
		filesToDelete = []string{
			TestCreateFile,
			TestGenerateSchemaFile,
			TestDumpFile,
			TestPreProcessFile,
			TestProcessDumpfile,
		}
	)
	for _, f := range filesToDelete {
    fi, err := os.Stat(f)
		if os.IsNotExist(err) || ! fi.IsDir() {
			continue
		} else {
			assert.Nil(t, os.Remove(f))
		}
	}
}

