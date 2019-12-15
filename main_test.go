package gonymizer

import (
	"os"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

/*
	NOTICE: This is the main file for driving all testing
*/

// Test Databases
const TestDb = "gon_test_db"
const TestPiiDb = "gon_generator_test_pii"
const TestPostLocalDb = "gon_generator_test_postprocess"
const TestLoadFileDb = "gon_loader_test"

// Input Test Files
const TestDbFile = "testing/test_db.sql"
const TestMapFile = "testing/test_map.json"
const TestPreProcessFile = "testing/test_pre_process.sql"
const TestPostProcessFile = "testing/test_post_process.sql"
const TestSQLCommandFile = "testing/test_sql_command_file.sql"
const TestRowCountFile = "testing/test_row_counts.csv"
const TestRowCountIncorrectRowCountsFile = "testing/test_row_counts_incorrect_row_counts.csv"
const TestRowCountsIncorrectNumberColumnsFile = "testing/test_row_counts_incorrect_number_columns.csv"

// Output test files
const TestCreateFile = "testing/output.TestCreateFile.sql"
const TestDumpFile = "testing/output.TestDumpFile.sql"
const TestGenerateSchemaFile = "testing/output.TestGenerateSchemaFile.sql"
const TestMapOutputFile = "testing/output.TestMapperFile.json"
const TestFileInjectorFile = "testing/output.TestFileInjectorFile.sql"
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
)

// GetTestDbConf will return a PGConfig for the specified localhost testing database.
func GetTestDbConf(dbName string) PGConfig {
	host, ok := os.LookupEnv("POSTGRES_HOST")
	if !ok {
		host = "localhost"
	}
	conf := PGConfig{}
	conf.Username = os.Getenv("POSTGRES_USER")
	conf.Host = host
	conf.DefaultDBName = dbName
	conf.SSLMode = "disable"
	return conf
}

// LoadTestDb will load the test database into the supplied database name on localhost.
func LoadTestDb(dbName string) error {
	conf := GetTestDbConf(dbName)

	// Prep database -- ignore if the db does not exist
	_ = DropDatabase(conf)
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
	return DropDatabase(conf)
}

// TestStart is the main entry point for all tests. Testing should be started using the Go test -run TestStart
// command.
func TestStart(t *testing.T) {
	// ****************************** NOTICE TO TESTERS / DEVELOPERS *************************************************
	// Please remember to always checkin logrus.FatalLevel
	// otherwise there is noise in the CI logs.
	//
	// To change LogLevel:
	// Info -> logrus.SetLevel(logrus.InfoLevel)
	// Fatal -> logrus.SetLevel(logrus.FatalLevel)
	logrus.SetLevel(logrus.FatalLevel)
	removeTestFiles(t)
	seqUnitTests(t)
}

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
	t.Run("ProcessorIPv4", TestProcessorIPv4)
	t.Run("ProcessorLastName", TestProcessorLastName)
	t.Run("ProcessorPhoneNumber", TestProcessorPhoneNumber)
	t.Run("ProcessorState", TestProcessorState)
	t.Run("ProcessorStateAbbrev", TestProcessorStateAbbrev)
	t.Run("ProcessorUserName", TestProcessorUserName)
	t.Run("ProcessorZip", TestProcessorZip)
	t.Run("ProcessorCompanyName", TestProcessorCompanyName)
	t.Run("ProcessorRandomDate", TestProcessorRandomDate)
	t.Run("ProcessorRandomUUID", TestProcessorRandomUUID)
	t.Run("ProcessorScrubString", TestProcessorScrubString)
	t.Run("randomizeUUID", TestRandomizeUUID)

	// Below are tests that require the test database to be loaded into Postgres for testing functionality. This requires
	// one to update the map file as well as create fake data in the testing/test_db.sql file when  updating users
	t.Run("CreateDatabase", TestCreateDatabase)
	require.Nil(t, LoadTestDb(TestDb))
	t.Run("CreateDumpFile", TestCreateDumpFile)

	// Load test database for testing sequential tests
	t.Run("PsqlCommand", TestPsqlCommand)
	t.Run("SQLCommandFile", TestSQLCommandFileFunc)

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
		if os.IsNotExist(err) || !fi.IsDir() {
			continue
		} else {
			t.Logf("Removing file: %s", f)
			require.Nil(t, os.Remove(f))
		}
	}
}
