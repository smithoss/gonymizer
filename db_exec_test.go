package gonymizer

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCreateDatabase(t *testing.T) {
	conf := GetTestDbConf(TestDb)
	assert.Nil(t, CreateDatabase(conf))
}

func TestDropDatabase(t *testing.T) {
	conf := GetTestDbConf(TestDb)

	// We need to make sure no one is connected to the database before dropping
	psqlDbConf := GetTestDbConf(TestDb)
	psqlDbConf.DefaultDBName = "postgres"
	psqlConn, err := OpenDB(psqlDbConf)
	assert.Nil(t, err)
	err = KillDatabaseConnections(psqlConn, conf.DefaultDBName)
	if err != nil && err.Error() != "sql: no rows in result set" {
		assert.Nil(t, err)
	}

	// Now drop the database
	assert.Nil(t, DropDatabase(conf))
}

func TestPsqlCommand(t *testing.T) {
	conf := GetTestDbConf(TestDb)
	dburl := conf.BaseURI()
	cmd := "psql"
	args := []string{
		dburl,
		"-c", // run a command
		` SELECT table_catalog, table_schema, table_name, column_name, data_type, ordinal_position,
			CASE
			    WHEN is_nullable = 'YES' THEN
			        TRUE
          WHEN is_nullable = 'NO' THEN
              FALSE
					END AS is_nullable
			FROM information_schema.columns
			WHERE table_schema = 'public'
			ORDER BY table_name, ordinal_position;`,
	}
	assert.Nil(t, ExecPostgresCmd(cmd, args...))
}
