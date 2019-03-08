package gonymizer

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func exampleConfigs() []PGConfig {

	return []PGConfig{
		// postgres://localhost
		// postgres://localhost:5433
		// postgres://localhost/mydb
		{Host: "localhost"},
		{Host: "localhost:5433"},
		{Host: "localhost", DefaultDBName: "mydb"},

		// postgres://user@localhost
		// postgres://user:secret@localhost
		{Host: "localhost", Username: "user"},
		{Host: "localhost", Username: "user", Pass: "secret"},

		// postgres://other@localhost/otherdb?sslmode=disable
		// postgres://other@localhost/?sslmode=disable
		{Host: "localhost", Username: "other", DefaultDBName: "otherdb", SSLMode: "disable"},
		{Host: "localhost", Username: "other", SSLMode: "disable"},

		// postgres://other:secret@example.com:123
		// postgres://other:secret@example.com:123/?sslmode=disable
		{Host: "example.com:123", Username: "other", Pass: "secret"},
		{Host: "example.com:123", Username: "other", Pass: "secret", SSLMode: "disable"},
	}
}

func TestDSN(t *testing.T) {

	fieldsToChecks := []Checker{
		{Label: "", Expected: "postgres://localhost"},
		{Label: "", Expected: "postgres://localhost:5433"},
		{Label: "", Expected: "postgres://localhost/mydb"},
		{Label: "", Expected: "postgres://user@localhost"},
		{Label: "", Expected: "postgres://user:secret@localhost"},
		{Label: "", Expected: "postgres://other@localhost/otherdb?sslmode=disable"},
		{Label: "", Expected: "postgres://other@localhost/?sslmode=disable"},
		{Label: "", Expected: "postgres://other:secret@example.com:123"},
		{Label: "", Expected: "postgres://other:secret@example.com:123/?sslmode=disable"},
	}

	confs := exampleConfigs()

	for i := 0; i < len(fieldsToChecks); i++ {
		checker := fieldsToChecks[i]
		checker.Candidate = confs[i].DSN()
		fieldsToChecks[i] = checker
	}

	CheckAll(t, fieldsToChecks)
}

func TestBaseDSN(t *testing.T) {

	fieldsToChecks := []Checker{
		{Label: "", Expected: "postgres://localhost"},
		{Label: "", Expected: "postgres://localhost:5433"},
		{Label: "", Expected: "postgres://localhost"},
		{Label: "", Expected: "postgres://user@localhost"},
		{Label: "", Expected: "postgres://user:secret@localhost"},
		{Label: "", Expected: "postgres://other@localhost/?sslmode=disable"},
		{Label: "", Expected: "postgres://other@localhost/?sslmode=disable"},
		{Label: "", Expected: "postgres://other:secret@example.com:123"},
		{Label: "", Expected: "postgres://other:secret@example.com:123/?sslmode=disable"},
	}

	confs := exampleConfigs()

	for i := 0; i < len(fieldsToChecks); i++ {
		checker := fieldsToChecks[i]
		checker.Candidate = confs[i].BaseDSN()
		fieldsToChecks[i] = checker
	}

	CheckAll(t, fieldsToChecks)
}

func TestURI(t *testing.T) {

	fieldsToChecks := []Checker{
		{Label: "", Expected: "postgres://localhost"},
		{Label: "", Expected: "postgres://localhost:5433"},
		{Label: "", Expected: "postgres://localhost/mydb"},
		{Label: "", Expected: "postgres://user@localhost"},
		{Label: "", Expected: "postgres://user:secret@localhost"},
		{Label: "", Expected: "postgres://other@localhost/otherdb?sslmode=disable"},
		{Label: "", Expected: "postgres://other@localhost/?sslmode=disable"},
		{Label: "", Expected: "postgres://other:secret@example.com:123"},
		{Label: "", Expected: "postgres://other:secret@example.com:123/?sslmode=disable"},
	}

	confs := exampleConfigs()

	for i := 0; i < len(fieldsToChecks); i++ {
		checker := fieldsToChecks[i]
		checker.Candidate = confs[i].URI()
		fieldsToChecks[i] = checker
	}

	CheckAll(t, fieldsToChecks)
}

func TestBaseURI(t *testing.T) {

	fieldsToChecks := []Checker{
		{Label: "", Expected: "postgres://localhost"},
		{Label: "", Expected: "postgres://localhost:5433"},
		{Label: "", Expected: "postgres://localhost"},
		{Label: "", Expected: "postgres://user@localhost"},
		{Label: "", Expected: "postgres://user:secret@localhost"},
		{Label: "", Expected: "postgres://other@localhost/?sslmode=disable"},
		{Label: "", Expected: "postgres://other@localhost/?sslmode=disable"},
		{Label: "", Expected: "postgres://other:secret@example.com:123"},
		{Label: "", Expected: "postgres://other:secret@example.com:123/?sslmode=disable"},
	}

	confs := exampleConfigs()

	for i := 0; i < len(fieldsToChecks); i++ {
		checker := fieldsToChecks[i]
		checker.Candidate = confs[i].BaseURI()
		fieldsToChecks[i] = checker
	}

	CheckAll(t, fieldsToChecks)
}

func TestOpenDB(t *testing.T) {
	conf := GetTestDbConf(TestDb)
	dbConn, err := OpenDB(conf)
	assert.Nil(t, err)
	assert.NotNil(t, dbConn)
	result := dbConn.QueryRow("SELECT count(*) FROM information_schema.columns")
	assert.NotNil(t, result)
}
