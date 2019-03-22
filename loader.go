package gonymizer

import (
	"database/sql"
	"encoding/csv"
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
	"io"
	"os"
	"strconv"
	"time"
)

// LoadFile will load an SQL file into the specified PGConfig.
func LoadFile(conf PGConfig, filePath string) (err error) {
	var (
		dbExists   bool
		mainConn   *sql.DB
		tempDbConf PGConfig
		psqlDbConf PGConfig
	)

	// Build the temporary databadse config. This is where we will load the new data to minimize
	// downtime during the reload.
	tempDbConf = conf
	tempDbConf.DefaultDBName = conf.DefaultDBName + "_gonymizer_loading"

	mainConn, err = OpenDB(conf)
	if err != nil {
		return err
	}
	defer mainConn.Close()

	// It is always good to check to see if a previous version of the gonymizer table still exists
	log.Infof("Checking to see if database '%s' exists", tempDbConf.DefaultDBName)
	dbExists, err = CheckIfDbExists(mainConn, tempDbConf.DefaultDBName)
	if err != nil {
		return err
	} else if dbExists {
		return fmt.Errorf("Found a previous version of the %s database. Is there another copy "+
			"of Gonymizer running?", tempDbConf.DefaultDBName)
	}

	// Create temp database
	log.Info("Creating database: ", tempDbConf.DefaultDBName)
	if err = CreateDatabase(tempDbConf); err != nil {
		log.Error("Unable to create database: ", tempDbConf.DefaultDBName)
		return err
	}

	log.Infof("Reloading database file '%s' -> '%s' ", filePath, tempDbConf.DefaultDBName)
	if err = SQLCommandFile(tempDbConf, filePath, true); err != nil {
		log.Fatalf("There was an error importing '%s' to: %s", filePath, tempDbConf.DefaultDBName)
		return err
	}

	// Kill all database connections so we can swap the databases
	// Reload the database into the new temp db
	psqlDbConf = conf
	psqlDbConf.DefaultDBName = "postgres"
	psqlConn, err := OpenDB(psqlDbConf)
	if err != nil {
		return err
	}

	// Kill db connections so we can rename the database
	log.Info("Killing all connections on database: ", conf.DefaultDBName)
	if err = KillDatabaseConnections(psqlConn, conf.DefaultDBName); err != nil {
		log.Error("Unable to kill connections on database: ", psqlDbConf.DefaultDBName)
		return err
	}

	// Rename main database -> old database
	oldDbName := conf.DefaultDBName + "_old_" + strconv.FormatInt(time.Now().Unix(), 10)

	log.Infof("Renaming database '%s' -> '%s'", conf.DefaultDBName, oldDbName)
	if err = RenameDatabase(psqlConn, conf.DefaultDBName, oldDbName); err != nil {
		return err
	}

	// Rename temp database -> main database
	log.Infof("Renaming database '%s' -> '%s'", tempDbConf.DefaultDBName, conf.DefaultDBName)
	if err = RenameDatabase(psqlConn, tempDbConf.DefaultDBName, conf.DefaultDBName); err != nil {
		return err
	}

	return nil
}

// VerifyRowCount will verify that the rowcounts in the PGConfig matches the supplied CSV file (see command/dump)
func VerifyRowCount(conf PGConfig, filePath string) (err error) {
	// Load local row counts into a map of maps so we can quickly look up values
	dbRowCount := make(map[string]map[string]int)
	rowObjs, err := GetTableRowCountsInDB(conf, "", []string{})
	if err != nil {
		return err
	}

	for _, row := range *rowObjs {
		if len(dbRowCount[*row.SchemaName]) < 1 {
			dbRowCount[*row.SchemaName] = make(map[string]int)
			dbRowCount[*row.SchemaName][*row.TableName] = *row.Count
		} else {
			dbRowCount[*row.SchemaName][*row.TableName] = *row.Count
		}
	}

	// No read in CSV file and compare to our DB counts
	reader, err := os.OpenFile(filePath, os.O_RDONLY, 0644)
	if err != nil {
		return err
	}
	csvReader := csv.NewReader(reader)
	lineNum := 1

	// Now loop through CSV and verify our count matches the CSV
	for {
		csvRow, err := csvReader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		// Verify we have schema, table, count
		if len(csvRow) != 3 {
			e := fmt.Sprint("CSV should contain exactly 3 columns, but has ", len(csvRow))
			return errors.New(e)
		}

		// Carve out data from CSV
		schema := csvRow[0]
		table := csvRow[1]
		count, err := strconv.Atoi(csvRow[2])
		if err != nil {
			return err
		}

		if len(csvRow) != 3 {
			return errors.New("CSV file had the wrong number of columns")
		}
		// Now check to see if they match

		if dbRowCount[schema][table] != count {
			log.Warnf("Production row counts do not match: (prod) %s.%s = %d / %d",
				schema,
				table,
				count,
				dbRowCount[schema][table],
			)
		}
		lineNum++
	}
	return err
}
