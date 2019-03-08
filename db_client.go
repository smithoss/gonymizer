package gonymizer

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/lib/pq"
	log "github.com/sirupsen/logrus"
	"strings"
)


// RowCounts is used to keep track of the number of rows for a given schema and table.
type RowCounts struct {
	SchemaName *string
	TableName  *string
	Count      *int
}

// CheckIfDbExists checks to see if the database exists using the provided db connection.
func CheckIfDbExists(db *sql.DB, dbName string) (exists bool, err error) {
	s := "SELECT exists(SELECT datname FROM pg_catalog.pg_database WHERE lower(datname) = lower($1));"
	row := db.QueryRow(s, dbName)
	_ = row.Scan(&exists)
	log.Debugf("Exists: %t", exists)
	return exists, err
}

// GetAllProceduresInSchema will return all procedures for the given schemas in SQL form.
func GetAllProceduresInSchema(conf PGConfig, schema string) ([]string, error) {
	var (
		rows       *sql.Rows
		procedures []string
	)
	db, err := OpenDB(conf)
	if err != nil {
		log.Error(err)
		return nil, err
	}
	defer db.Close()

	rows, err = db.Query(`
		SELECT pg_get_functiondef(f.oid)
		FROM pg_catalog.pg_proc f
		INNER JOIN pg_catalog.pg_namespace n ON (f.pronamespace = n.oid)
		WHERE n.nspname = $1`, schema)

	if err != nil {
		log.Error(err)
		return nil, err
	}
	defer rows.Close()

	for {
		var procedure string
		for rows.Next() {
			_ = rows.Scan(&procedure)
			procedures = append(procedures, procedure)
		}

		if !rows.NextResultSet() {
			break
		}
	}

	return procedures, nil
}

// GetAllSchemaColumns will return a row pointer to a list of table and column names for the given database connection.
func GetAllSchemaColumns(db *sql.DB) (*sql.Rows, error) {
	query := `
			SELECT table_catalog, table_schema, table_name, column_name, data_type, ordinal_position, is_nullable
			FROM information_schema.columns
			WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
			ORDER BY table_schema, table_name, ordinal_position
	`
	rows, err := db.Query(query)

	if err != nil {
		log.Error(err)
		return nil, err
	}
	return rows, nil
}

// GetAllTablesInSchema will return a list of database tables for a given database configuration.
func GetAllTablesInSchema(conf PGConfig, schema string) ([]string, error) {
	var (
		rows       *sql.Rows
		tableNames []string
	)

	db, err := OpenDB(conf)
	if err != nil {
		log.Error(err)
		return nil, err
	}
	defer db.Close()

	// Set default to the public schema
	if len(schema) < 1 {
		schema = "public"
	}

	rows, err = db.Query(`
	SELECT table_name
	FROM information_schema.tables
	WHERE table_schema = $1`,
		schema,
	)
	if err != nil {
		log.Error(err)
		return nil, err
	}

	defer rows.Close()

	for {
		var tableName string
		for rows.Next() {
			_ = rows.Scan(&tableName)

			tableNames = append(tableNames, tableName)
		}

		if !rows.NextResultSet() {
			break
		}
	}

	return tableNames, nil
}

// GetSchemasInDatabase returns a list of schemas for a given database configuration. If an excludeSchemas list is
// provided GetSchemasInDatabase will leave them out of the returned list of schemas.
func GetSchemasInDatabase(conf PGConfig, excludeSchemas []string) ([]string, error) {
	var (
		rows            *sql.Rows
		includedSchemas []string
	)

	db, err := OpenDB(conf)
	if err != nil {
		log.Error(err)
		return nil, err
	}
	defer db.Close()

	rows, err = db.Query(`
		SELECT schema_name
		FROM information_schema.schemata
		WHERE schema_name NOT IN ($1)`, pq.Array(excludeSchemas))
	defer rows.Close()

	if err != nil {
		log.Error("Query IN clause: ")
		log.Error(err)
		return nil, err
	}

	for {
		var schema string
		for rows.Next() {
			found := false
			_ = rows.Scan(&schema)
			for _, ecs := range excludeSchemas {
				if ecs == schema {
					found = true
				}
			}
			if !found {
				includedSchemas = append(includedSchemas, schema)
			}
		}

		if !rows.NextResultSet() {
			break
		}
	}

	return includedSchemas, nil
}

// GetSchemaColumnEquals returns a pointer to a list of database rows containing the names of tables and columns for
// the provided schema (using the SQL equals operator).
func GetSchemaColumnEquals(db *sql.DB, schema string) (*sql.Rows, error) {
	rows, err := db.Query(`
	SELECT table_catalog, table_schema, table_name, column_name, data_type, ordinal_position, is_nullable
	FROM information_schema.columns
	WHERE table_schema = $1
	ORDER BY table_schema, table_name, ordinal_position`, schema)

	if err != nil {
		log.Error(err)
		return nil, err
	}
	return rows, nil
}

// GetSchemaColumnsLike will return a pointer to a list of database rows containing the names of tables and columns for
// the provided schema (using the SQL LIKE operator).
func GetSchemaColumnsLike(db *sql.DB, schemaPrefix string) (*sql.Rows, error) {
	var selectedSchema string

	// NOTE: Since we are grabbing a schema that matches the schemaPrefix we will assume UNIFORMITY in the DDL across all
	// tables in each schema that match the prefix. Following this requirement, we can assume that we only need to grab a
	// single schema that matches the prefix and use it as the map for all schemas that match the schemaPrefix.
	err := db.QueryRow("SELECT table_schema FROM information_schema.columns WHERE table_schema LIKE $1 LIMIT 1",
		schemaPrefix+"%").Scan(&selectedSchema)
	switch err {
	case sql.ErrNoRows:
		fmt.Println("No rows were returned!")
	case nil:
		break
	default:
		panic(err)
	}

	// Now grab all the columns from this schema
	rows, err := db.Query(`
			SELECT table_catalog, table_schema, table_name, column_name, data_type, ordinal_position, is_nullable
			FROM information_schema.columns
			WHERE table_schema = $1
			ORDER BY table_schema, table_name, ordinal_position`, selectedSchema)

	if err != nil {
		log.Error(err)
		return nil, err
	}
	return rows, nil

}

// GetTableRowCountsInDB collects the number of rows for each table in the given supplied schema prefix and will not
// include any of the tables listed in the excludeTable list. Returns a list of tables the number of rows for each.
func GetTableRowCountsInDB(conf PGConfig, schemaPrefix string, excludeTable []string) (*[]RowCounts, error) {
	var (
		rows        *sql.Rows
		dbRowCounts []RowCounts
	)

	db, err := OpenDB(conf)
	if err != nil {
		log.Error(err)
		return nil, err
	}
	defer db.Close()

	// Get a list of all schemas + tables in the database (excluding excludeTable)
	query := `
		SELECT schemaname, tablename
		FROM pg_catalog.pg_tables
		WHERE schemaname NOT LIKE 'pg_%'
			AND schemaname != 'information_schema'
	`
	if len(excludeTable) > 0 {
		query += "          AND tablename NOT IN ($1)"
		query += "\n          ORDER BY schemaname, tablename;"
		rows, err = db.Query(query, pq.Array(excludeTable))
	} else {
		query += "          ORDER BY schemaname, tablename;"
		rows, err = db.Query(query)

	}

	if err != nil {
		return nil, err
	} else if rows == nil {
		return nil, errors.New("Returned 0 tables in " + conf.DefaultDBName + ".")
	}

	// Build array string to pass into query (Injection Safe)
	// See: https://groups.google.com/forum/#!msg/golang-nuts/vHbg09g7s2I/RKU7XsO25SIJ
	if err != nil {
		return nil, err
	}
	for {
		for rows.Next() {
			var (
				schemaName string
				tableName  string
				count      int
				exclude    bool
			)
			count = 0
			exclude = false

			_ = rows.Scan(&schemaName, &tableName)

			// Search exclude list to see if schema + table are in it. if so skip them
			//TODO: Refactor this to use efficent search (key lookups are possible)
			for _, e := range excludeTable {
				s := strings.Split(e, ".")
				if len(schemaPrefix) > 0 && strings.HasPrefix(s[0], schemaPrefix) && s[1] == tableName {
					exclude = true
					break
				} else if schemaName == s[0] && tableName == s[1] {
					exclude = true
					break
				}
			}
			if !exclude {
				dbRowCounts = append(dbRowCounts, RowCounts{SchemaName: &schemaName, TableName: &tableName, Count: &count})
			}
		}
		if !rows.NextResultSet() {
			break
		}
	}

	// Luckily Postgres is smart and does not blow away cache for a
	// simple Count(*). See -> https://stackoverflow.com/questions/37097736/understanding-postgres-caching
	for _, row := range dbRowCounts {
		query := fmt.Sprintf("SELECT COUNT(*) FROM %s.%s;", *row.SchemaName, *row.TableName)
		if err := db.QueryRow(query).Scan(row.Count); err != nil {
			log.Error(err)
		}
	}
	return &dbRowCounts, err
}

// KillDatabaseConnections will kill all connections to the provided database name.
func KillDatabaseConnections(db *sql.DB, dbName string) (err error) {
	var success string

	query := `
	SELECT pg_terminate_backend(pid) 
	FROM pg_stat_activity 
	WHERE pid != pg_backend_pid()
		AND datname = $1;`

	err = db.QueryRow(query, dbName).Scan(&success)
	if err != nil {
		log.Error(err)
	}
	log.Debug("Success: ", success)
	return err
}

// RenameDatabase will rename a database using the fromName to the toName.
func RenameDatabase(db *sql.DB, fromName, toName string) (err error) {
	_, err = db.Exec(fmt.Sprintf("ALTER DATABASE %s RENAME TO %s", fromName, toName))
	if err != nil {
		log.Errorf("Unable to rename database '%s' -> '%s'", fromName, toName)
		log.Error(err)
		return err
	}
	return err
}
