package gonymizer

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
)

// ProcessorDefinition is the processor data structure used to map database columns to their specified column processor.
type ProcessorDefinition struct {
	Name string

	// optional helpers
	Max      float64
	Min      float64
	Variance float64

	// values that match this regex will not be anonymized
	Exemptions string

	Comment string
}

// ColumnMapper is the data structure that contains all gonymizer required information for the specified column.
type ColumnMapper struct {
	Comment         string
	TableSchema     string
	TableName       string
	ColumnName      string
	DataType        string
	ParentSchema    string
	ParentTable     string
	ParentColumn    string
	OrdinalPosition int

	IsNullable bool

	Processors []ProcessorDefinition
}

// DBMapper is the main structure for the map file JSON object and is used to map all database columns that will be
// anonymized.
type DBMapper struct {
	DBName       string
	SchemaPrefix string
	Seed         int64
	ColumnMaps   []ColumnMapper
}

// ColumnMapper returns the address of the ColumnMapper object if it matches the given parameters otherwise it returns
// nil. Special cases exist for sharded schemas using the schema-prefix. See documentation for details.
func (dbMap DBMapper) ColumnMapper(schemaName, tableName, columnName string) *ColumnMapper {

	// Some names may contain quotes if the name is a reserved word. For example tableName public.order would be a
	// conflict with ORDER BY so PSQL will add quotes to the name. I.E. public."order". Remove the quotes so we can match
	// whatever is in the map file.
	schemaName = strings.Replace(schemaName, "\"", "", -1)
	tableName = strings.Replace(tableName, "\"", "", -1)
	columnName = strings.Replace(columnName, "\"", "", -1)

	for _, cmap := range dbMap.ColumnMaps {
		//log.Infoln("dbMap.SchemaPrefix-> ", dbMap.SchemaPrefix)
		//log.Infoln("schemaName-> ", schemaName)

		if len(dbMap.SchemaPrefix) > 0 && strings.HasPrefix(schemaName, dbMap.SchemaPrefix) && cmap.TableName == tableName &&
			cmap.ColumnName == columnName {
			return &cmap
		} else if cmap.TableSchema == schemaName && cmap.TableName == tableName && cmap.ColumnName == columnName {
			return &cmap
		}
	}
	return nil
}

// Validate is used to verify that a database map is complete and correct.
func (dbMap *DBMapper) Validate() error {
	if len(dbMap.DBName) == 0 {
		return errors.New("Expected non-empty DBName")
	}
	// Ensure that each processor is defined
	for _, columnMap := range dbMap.ColumnMaps {
		for _, processor := range columnMap.Processors {
			if _, ok := ProcessorCatalog[processor.Name]; !ok {
				return fmt.Errorf("Unrecognized Processor %s", processor.Name)
			}
		}
	}

	return nil
}

// GenerateConfigSkeleton will generate a column-map based on the supplied PGConfig and previously configured map file.
func GenerateConfigSkeleton(conf PGConfig, schemaPrefix string, schemas, excludeTables []string) (*DBMapper, error) {
	var (
		dbmap     *DBMapper
		columnMap []ColumnMapper
	)
	db, err := OpenDB(conf)
	if err != nil {
		log.Error(err)
		return nil, err
	}

	dbmap = new(DBMapper)
	dbmap.DBName = conf.DefaultDBName
	dbmap.SchemaPrefix = schemaPrefix

	columnMap = []ColumnMapper{}

	if len(schemas) < 1 {
		schemas = append(schemas, "public")
	}

	log.Info("Schemas to map: ", schemas)
	for _, schema := range schemas {
		log.Info("Mapping columns for schema: ", schema)
		columnMap, err = mapColumns(db, columnMap, schemaPrefix, schema, excludeTables)
		if err != nil {
			return nil, err
		}
	}
	dbmap.ColumnMaps = columnMap
	return dbmap, nil
}

// WriteConfigSkeleton will save the supplied DBMap to filepath.
func WriteConfigSkeleton(dbmap *DBMapper, filepath string) error {

	f, err := os.Create(filepath)
	if err != nil {
		log.Error("Failure to open file: ", err)
		log.Error("filepath: ", filepath)
		return err
	}
	defer f.Close()

	jsonEncoder := json.NewEncoder(f)
	jsonEncoder.SetIndent("", "    ")

	err = jsonEncoder.Encode(dbmap)
	if err != nil {
		log.Error(err)
		log.Error("filepath", filepath)
		return err
	}

	return nil
}

// LoadConfigSkeleton will load the column-map into memory for use in dumping, processing, and loading of SQL files.
func LoadConfigSkeleton(givenPathToFile string) (*DBMapper, error) {
	pathToFile := givenPathToFile

	f, err := os.Open(pathToFile)
	if err != nil {
		log.Error("Failure to open file: ", err)
		log.Error("givenPathToFile: ", givenPathToFile)
		log.Error("pathToFile: ", pathToFile)
		return nil, err
	}
	defer f.Close()

	jsonDecoder := json.NewDecoder(f)

	dbmap := new(DBMapper)
	err = jsonDecoder.Decode(dbmap)
	if err != nil {
		log.Error(err)
		log.Error("givenPathToFile: ", givenPathToFile)
		log.Error("pathToFile: ", pathToFile)
		log.Error("f: ", f)
		return nil, err
	}

	err = dbmap.Validate()
	if err != nil {
		log.Error(err)
		log.Error("dbmap: ", dbmap)
		return nil, err
	}

	return dbmap, nil
}

// findColumn searches the in-memory loaded column map using the specified parameters.
func findColumn(columns []ColumnMapper, columnName, tableName, schemaPrefix, schema,
	dataType string) (col ColumnMapper) {

	for _, col = range columns {

		// Regular Column
		if col.ColumnName == columnName && col.TableName == tableName && col.TableSchema == schema &&
			col.DataType == dataType {
			return col

			// Sharded Column
		} else if col.ColumnName == columnName && col.TableName == tableName && col.TableSchema == schemaPrefix+"*" &&
			col.DataType == dataType {
			return col
		}
	}
	return ColumnMapper{}
}

// addColumn creates a ColumnMapper structure based on the input parameters.
func addColumn(columnName, tableName, schema, dataType string, ordinalPosition int,
	isNullable bool, relationRows []map[string]interface{}) ColumnMapper {
	col := ColumnMapper{}

	for _, value := range relationRows {
		if value["table_name"] == tableName && value["column_name"] == columnName {
			col.ParentTable = value["foreign_table_name"].(string)
			col.ParentColumn = value["foreign_column_name"].(string)
			col.ParentSchema = value["table_schema"].(string)
		}
	}

	col.Processors = []ProcessorDefinition{
		{
			Name: "Identity",
		},
	}
	col.TableName = tableName
	col.ColumnName = columnName
	col.DataType = dataType
	col.OrdinalPosition = ordinalPosition
	col.IsNullable = isNullable
	col.TableSchema = schema

	return col
}

func ProcessRowToMap(rows *sql.Rows) []map[string]interface{} {
	returnedColumns, err := rows.Columns()

	scanArgs := make([]interface{}, len(returnedColumns))
	values := make([]interface{}, len(returnedColumns))

	results := make([]map[string]interface{}, 0)

	for i := range values {
		scanArgs[i] = &values[i]
	}

	for rows.Next() {
		err = rows.Scan(scanArgs...)
		if err != nil {
			panic(err)
		}

		record := make(map[string]interface{})

		for i, col := range values {
			if col != nil {
				switch col.(type) {
				case bool:
					record[returnedColumns[i]] = col.(bool)
				case int:
					record[returnedColumns[i]] = col.(int)
				case int64:
					record[returnedColumns[i]] = col.(int64)
				case float64:
					record[returnedColumns[i]] = col.(float64)
				case string:
					record[returnedColumns[i]] = col.(string)
				case time.Time:
					record[returnedColumns[i]] = col.(time.Time)
				case []byte:
					record[returnedColumns[i]] = string(col.([]byte))
				default:
					record[returnedColumns[i]] = col
				}
			}
		}

		results = append(results, record)
	}

	return results
}

// mapColumns
func mapColumns(db *sql.DB, columns []ColumnMapper, schemaPrefix, schema string,
	excludeTables []string) ([]ColumnMapper, error) {
	var (
		err           error
		rows          *sql.Rows
		prefixPresent bool
	)

	// Below is a high level state diagram based on the schema prefix and schema being supplied.
	// empty = empty string or ""
	// group_ = example schema prefix
	// public = example schema name
	// =====================================================
	//  Shard  | Schema | Outcome
	// -----------------------------------------------------
	//  empty  |  empty | Map All Schemas
	// -----------------------------------------------------
	//  empty  | public | Map only provided schema name
	// -----------------------------------------------------
	//  group_ |  empty | Invalid
	// -----------------------------------------------------
	//  group_ |  group | build single map for schema prefix
	// -----------------------------------------------------
	//  group_ | public | Map only provided schema
	// -----------------------------------------------------
	prefixPresent = false

	if len(schemaPrefix) == 0 && len(schema) == 0 {

		log.Debug("Mapping all schemas")
		rows, err = GetAllSchemaColumns(db)

	} else if len(schemaPrefix) == 0 && len(schema) > 0 {

		log.Debug("Mapping a single schema")
		rows, err = GetSchemaColumnEquals(db, schema)

	} else if schemaPrefix != "" && schema == "" {

		// Invalid
		return nil, errors.New("You cannot use SchemaPrefix option without a schema to map it to")

	} else if strings.HasPrefix(schemaPrefix, schema) {

		log.Debug("Mapping a schema with SchemaPrefix present")
		prefixPresent = true
		rows, err = GetSchemaColumnsLike(db, schemaPrefix)

	} else {

		log.Debug("Mapping a single schema")
		rows, err = GetSchemaColumnEquals(db, schema)

	}
	defer rows.Close()

	tablesNameRow, err := GetTablesName(db)

	var tableName string
	tableCollectionString := ""

	for tablesNameRow.Next() {
		err = tablesNameRow.Scan(
			&tableName,
		)

		tableCollectionString = tableCollectionString + "'" + tableName + "',"
	}

	tableCollectionString = "(" + tableCollectionString[:len(tableCollectionString)-1] + ")"
	defer tablesNameRow.Close()

	relationRows, err := GetRelationalColumns(db, tableCollectionString)

	defer relationRows.Close()

	processedRowToMap := ProcessRowToMap(relationRows)
	log.Debug("Iterating through rows and creating skeleton map")
	for {
		var (
			tableCatalog    string
			tableSchema     string
			tableName       string
			columnName      string
			dataType        string
			ordinalPosition int
			isNullable      bool
			exclude         bool
			col             ColumnMapper
		)

		// Iterate through each row and add the columns
		for rows.Next() {
			err = rows.Scan(
				&tableCatalog,
				&tableSchema,
				&tableName,
				&columnName,
				&dataType,
				&ordinalPosition,
				&isNullable,
			)

			// If we are working on a schema prefix, make sure to use the schema prefix + * as a name, otherwise empty
			if prefixPresent {
				tableSchema = schemaPrefix + "*"
			} else {
				schemaPrefix = ""
			}

			// check to see if table is in the list of skipped tables or data for the table (leave them out of map)
			exclude = false
			for _, item := range excludeTables {
				schemaTableName := fmt.Sprintf("%s.%s", tableSchema, tableName)
				if schemaTableName == item {
					exclude = true
					break
				}
			}
			if exclude {
				continue
			}

			// Search for columnName in columns, if the column exists in the dbmap leave as-is otherwise create a new one and
			// add to the column map
			col = findColumn(columns, columnName, tableName, schemaPrefix, schema, dataType)
			if col.TableSchema == "" && col.ColumnName == "" {
				col = addColumn(columnName, tableName, schema, dataType, ordinalPosition, isNullable, processedRowToMap)
				// Continuously append into the column map (old and new together)
				columns = append(columns, col)
			}
		}

		if !rows.NextResultSet() {
			break
		}
	}

	return columns, err
}
