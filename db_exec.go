package gonymizer

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

// DropDatabase will drop the database that is supplied in the PGConfig.
func DropDatabase(conf PGConfig) error {
	origName := conf.DefaultDBName
	conf.DefaultDBName = "postgres"
	dburl := conf.BaseURI()
	conf.DefaultDBName = origName

	cmd := "psql"
	args := []string{
		dburl,
		"-v", "ON_ERROR_STOP=1",
		"-c", // run a command
		"DROP DATABASE IF EXISTS " + conf.DefaultDBName + ";",
	}

	err := ExecPostgresCmd(cmd, args...)
	if err != nil {
		log.Error(err)
		log.Debug("dburl: ", dburl)
		return err
	}
	return nil
}

// DropPublicTables drops all tables in the public schema.
func DropPublicTables(conf PGConfig) error {
	tablenames, err := GetAllTablesInSchema(conf, "public")
	if err != nil {
		log.Error(err)
		return err
	}

	dropStatements := []string{}
	for _, tablename := range tablenames {
		dropStatements = append(dropStatements, fmt.Sprintf("DROP TABLE IF EXISTS \"%s\" CASCADE;", tablename))
	}

	dropAll := strings.Join(dropStatements, " ")

	cmd := "psql"
	args := []string{
		conf.URI(),
		"-v", "ON_ERROR_STOP=1",
		"-c", // run a command
		dropAll,
	}

	err = ExecPostgresCmd(cmd, args...)
	if err != nil {
		log.Error(err)
		log.Error("conf: ", conf)
		return err
	}

	return nil
}

// CreateDatabase will create the database that is supplied in the PGConfig.
func CreateDatabase(conf PGConfig) error {
	origName := conf.DefaultDBName
	// Always use postgres database when moving, creating, or destroying databases
	conf.DefaultDBName = "postgres"
	dburl := conf.URI()
	conf.DefaultDBName = origName

	cmd := "psql"
	args := []string{
		dburl,
		"-v", "ON_ERROR_STOP=1",
		"-c", // run a command
		"CREATE DATABASE " + conf.DefaultDBName + ";",
	}

	err := ExecPostgresCmd(cmd, args...)
	if err != nil {
		log.Error(err)
		log.Debug("dburl: ", dburl)
		return err
	}
	return nil
}

// SQLCommandFile will run psql -f on a file and execute any queries contained in the sql file. If ignoreErrors is
// supplied then psql will ignore errors in the file.
func SQLCommandFile(conf PGConfig, filepath string, ignoreErrors bool) error {

	dburl := conf.URI()

	cmd := "psql"
	args := []string{
		dburl,
	}

	// Should we quit on error?
	if !ignoreErrors {
		args = append(args, "-v", "ON_ERROR_STOP=1")
	}

	// Add the file to load last
	args = append(args, "-f", filepath)

	err := ExecPostgresCmd(cmd, args...)
	if err != nil {
		log.Error(err)
		log.Debug("dburl: ", dburl)
		return err
	}
	return nil
}

// ExecPostgresCmd executes the psql command, but first opens the db_test_*.log log files for debugging runtime
// issues using the psql command.
func ExecPostgresCmd(name string, args ...string) error {

	outLog := "db_test_out.log"

	outputFile, err := os.OpenFile(outLog, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0660)
	if err != nil {
		log.Error(err)
		log.Debug("outputFile: ", outLog)
		log.Debug("name: ", name)
		log.Debug("args: ", args)
		return err
	}
	defer outputFile.Close()

	errLog := "db_test_err.log"
	errorFile, err := os.OpenFile(errLog, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0660)
	if err != nil {
		log.Error(err)
		log.Debug("errorFile: ", errLog)
		log.Debug("name: ", name)
		log.Debug("args: ", args)
		return err
	}
	defer errorFile.Close()
	return ExecPostgresCommandOutErr(outputFile, errorFile, name, args...)
}

// ExecPostgresCommandOutErr is the executing function for the psql -f command. It also closed the loaded files/buffers
// from the calling functions.
func ExecPostgresCommandOutErr(stdOut, stdErr io.Writer, name string, arg ...string) error {
	var err error

	pgBinDir := viper.GetString("PG_BIN_DIR")
	if len(pgBinDir) > 0 {
		name = filepath.Join(pgBinDir, name)
	}
	cmd := exec.Command(name, arg...)
	cmd.Env = append(os.Environ())

	// we use buffers cause we want the output to go to two places...
	var outBuffer bytes.Buffer
	var errBuffer bytes.Buffer

	cmd.Stdout = &outBuffer
	cmd.Stderr = &errBuffer

	log.Debugf("Running command: %s %s", name, strings.Join(arg, " "))

	err = cmd.Run()
	outBytes := outBuffer.Bytes()
	errBytes := errBuffer.Bytes()

	stdOut.Write(outBytes)
	stdErr.Write(errBytes)

	if err != nil {
		log.Error(err)
		log.Debug("name: ", name)
		log.Debug("arg: ", arg)
		if len(errBytes) > 0 {
			log.Debugf("errBytes: \n=====================\n%s\n=====================\n", string(errBytes))
			log.Debugf("errBytes: \n=====================\n%s\n=====================\n", string(errBytes))
		}
		if len(outBytes) > 0 {
			log.Debugf("outBytes: \n=====================\n%s\n=====================\n", string(outBytes))
			log.Debugf("outBytes: \n=====================\n%s\n=====================\n", string(outBytes))
		}
	}
	return err
}
