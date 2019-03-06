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
		PGConfig{Host: "localhost"},
		PGConfig{Host: "localhost:5433"},
		PGConfig{Host: "localhost", DefaultDBName: "mydb"},

		// postgres://user@localhost
		// postgres://user:secret@localhost
		PGConfig{Host: "localhost", Username: "user"},
		PGConfig{Host: "localhost", Username: "user", Pass: "secret"},

		// postgres://other@localhost/otherdb?sslmode=disable
		// postgres://other@localhost/?sslmode=disable
		PGConfig{Host: "localhost", Username: "other", DefaultDBName: "otherdb", SSLMode: "disable"},
		PGConfig{Host: "localhost", Username: "other", SSLMode: "disable"},

		// postgres://other:secret@example.com:123
		// postgres://other:secret@example.com:123/?sslmode=disable
		PGConfig{Host: "example.com:123", Username: "other", Pass: "secret"},
		PGConfig{Host: "example.com:123", Username: "other", Pass: "secret", SSLMode: "disable"},
	}
}

func TestDSN(t *testing.T) {

	fieldsToChecks := []Checker{
		Checker{Label: "", Expected: "postgres://localhost"},
		Checker{Label: "", Expected: "postgres://localhost:5433"},
		Checker{Label: "", Expected: "postgres://localhost/mydb"},
		Checker{Label: "", Expected: "postgres://user@localhost"},
		Checker{Label: "", Expected: "postgres://user:secret@localhost"},
		Checker{Label: "", Expected: "postgres://other@localhost/otherdb?sslmode=disable"},
		Checker{Label: "", Expected: "postgres://other@localhost/?sslmode=disable"},
		Checker{Label: "", Expected: "postgres://other:secret@example.com:123"},
		Checker{Label: "", Expected: "postgres://other:secret@example.com:123/?sslmode=disable"},
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
		Checker{Label: "", Expected: "postgres://localhost"},
		Checker{Label: "", Expected: "postgres://localhost:5433"},
		Checker{Label: "", Expected: "postgres://localhost"},
		Checker{Label: "", Expected: "postgres://user@localhost"},
		Checker{Label: "", Expected: "postgres://user:secret@localhost"},
		Checker{Label: "", Expected: "postgres://other@localhost/?sslmode=disable"},
		Checker{Label: "", Expected: "postgres://other@localhost/?sslmode=disable"},
		Checker{Label: "", Expected: "postgres://other:secret@example.com:123"},
		Checker{Label: "", Expected: "postgres://other:secret@example.com:123/?sslmode=disable"},
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
		Checker{Label: "", Expected: "postgres://localhost"},
		Checker{Label: "", Expected: "postgres://localhost:5433"},
		Checker{Label: "", Expected: "postgres://localhost/mydb"},
		Checker{Label: "", Expected: "postgres://user@localhost"},
		Checker{Label: "", Expected: "postgres://user:secret@localhost"},
		Checker{Label: "", Expected: "postgres://other@localhost/otherdb?sslmode=disable"},
		Checker{Label: "", Expected: "postgres://other@localhost/?sslmode=disable"},
		Checker{Label: "", Expected: "postgres://other:secret@example.com:123"},
		Checker{Label: "", Expected: "postgres://other:secret@example.com:123/?sslmode=disable"},
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
		Checker{Label: "", Expected: "postgres://localhost"},
		Checker{Label: "", Expected: "postgres://localhost:5433"},
		Checker{Label: "", Expected: "postgres://localhost"},
		Checker{Label: "", Expected: "postgres://user@localhost"},
		Checker{Label: "", Expected: "postgres://user:secret@localhost"},
		Checker{Label: "", Expected: "postgres://other@localhost/?sslmode=disable"},
		Checker{Label: "", Expected: "postgres://other@localhost/?sslmode=disable"},
		Checker{Label: "", Expected: "postgres://other:secret@example.com:123"},
		Checker{Label: "", Expected: "postgres://other:secret@example.com:123/?sslmode=disable"},
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
