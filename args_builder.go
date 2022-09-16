package gonymizer

import (
	"fmt"
	"strings"
)

func CreateDumpArgs(
	conf PGConfig,
	dumpfilePath,
	schemaPrefix string,
	excludeTables,
	excludeDataTables,
	excludeCreateSchemas,
	schemas []string,
	oids bool,
) []string {

	args := []string{"--no-owner"}

	if oids {
		args = append(args, "--oids")
	}

	if len(schemas) >= 1 {
		// Add all schemas that match schemaPrefix to the dump list
		for _, s := range schemas {
			if strings.HasPrefix(schemaPrefix, s) {
				args = append(args, fmt.Sprintf("--schema=%s*", schemaPrefix))
			} else {
				args = append(args, fmt.Sprintf("--schema=%s", s))
			}
		}
	}

	// Exclude system schemas
	for _, sch := range excludeCreateSchemas {
		args = append(args, fmt.Sprintf("--exclude-schema=%s", sch))
	}

	// Exclude tables that are not needed (schema will not be dumped)
	for _, tbl := range excludeTables {
		// According to: https://www.postgresql.org/docs/9.3/static/app-pgdump.html we need to add a flag for every table
		// unless we use a regex match which we do not want in this case. Make sure to read the NOTES under --table. They
		// apply here as well.

		// tbl format => "schema_name.table_name"
		args = append(args, fmt.Sprintf("--exclude-table=%s", tbl))
	}

	// Exclude tables that we do not need data from (but keep the schema... restores a blank table)
	for _, tbl := range excludeDataTables {
		args = append(args, fmt.Sprintf("--exclude-table-data=%s", tbl))
	}

	args = append(args, "-f")
	args = append(args, dumpfilePath)

	// Always put URI last
	args = append(args, conf.URI())

	return args
}
