package main

import (
	"encoding/csv"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/logrusorgru/aurora"
	log "github.com/sirupsen/logrus"
	"github.com/smithoss/gonymizer"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// DumpCmd is the cobra.Command struct we use for "dump" command.
var (
	DumpCmd = &cobra.Command{
		Use:   "dump",
		Short: "Create a dump file that contains PHI/PII from a PostgreSQL database",
		Run:   cliCommandDump,
	}
)

// init initializes the Dump command for the application and adds application flags and options.
func init() {

	DumpCmd.Flags().BoolVarP(
		&dbDisableSSL,
		"disable-ssl",
		"S",
		false,
		"Disable SSL (Not-recommended)",
	)
	_ = viper.BindPFlag("dump.disable-ssl", DumpCmd.Flags().Lookup("disable-ssl"))

	DumpCmd.Flags().StringSliceVar(
		&excludeTable,
		"exclude-table",
		[]string{},
		"A table, or list of tables, that we do not want to include in the pg_dump (--exclude-table in pg_dump)",
	)
	_ = viper.BindPFlag("dump.exclude-table", DumpCmd.Flags().Lookup("exclude-table"))

	DumpCmd.Flags().StringSliceVar(
		&excludeTableData,
		"exclude-table-data",
		[]string{},
		"A table's data, or list of tables' data, that we do not want to include data in the dump "+
			"(--exclude-table-data in pg_dump)",
	)
	_ = viper.BindPFlag("dump.exclude-table-data", DumpCmd.Flags().Lookup("exclude-table-data"))

	DumpCmd.Flags().StringSliceVar(
		&excludeSchemas,
		"exclude-schemas",
		[]string{},
		"Schemas to skip DROP SCHEMA and CREATES SCHEMA for when creating the dump file. "+
			"NOTE: This is useful when using --schema-prefix and --schema to skip dropping/creating system schemas "+
			"such as 'public' which is done at initialization of the new anonymized database. See documentation.",
	)
	_ = viper.BindPFlag("dump.exclude-schemas", DumpCmd.Flags().Lookup("exclude-schemas"))

	DumpCmd.Flags().StringVarP(
		&dbHost,
		"host",
		"H",
		"",
		"Database host address",
	)
	_ = viper.BindPFlag("dump.host", DumpCmd.Flags().Lookup("host"))

	DumpCmd.Flags().StringVarP(
		&dbName,
		"database",
		"d",
		"",
		"Database name",
	)
	_ = viper.BindPFlag("dump.database", DumpCmd.Flags().Lookup("database"))

	DumpCmd.Flags().StringVar(
		&dumpFile,
		"dump-file",
		"",
		"Location to dump file containing PHI/PII",
	)
	_ = viper.BindPFlag("dump.dump-file", DumpCmd.Flags().Lookup("dump-file"))

	DumpCmd.Flags().StringSliceVar(
		&schema,
		"schema",
		[]string{},
		"Schema to dump to the SQL file. For example: --schema=public --schema=group --schema=share",
	)
	_ = viper.BindPFlag("dump.schema", DumpCmd.Flags().Lookup("schema"))

	DumpCmd.Flags().StringVar(
		&schemaPrefix,
		"schema-prefix",
		"",
		"The schema prefix for grouped schemas. I.E. --schema-prefix=mdb_ would match all 'mdb_*' "+
			"schemas in the catalog",
	)
	_ = viper.BindPFlag("map.schema-prefix", DumpCmd.Flags().Lookup("schema-prefix"))

	DumpCmd.Flags().StringVarP(
		&dbPassword,
		"password",
		"p",
		"",
		"Database password",
	)
	_ = viper.BindPFlag("dump.password", DumpCmd.Flags().Lookup("password"))

	DumpCmd.Flags().Int32VarP(
		&dbPort,
		"port",
		"P",
		5432,
		"Database port",
	)
	_ = viper.BindPFlag("dump.port", DumpCmd.Flags().Lookup("port"))

	DumpCmd.Flags().StringVar(
		&rowCountFile,
		"row-count-file",
		"",
		"CSV file to store table row counts to (see documentation)",
	)
	_ = viper.BindPFlag("dump.row-count-file", DumpCmd.Flags().Lookup("row-count-file"))

	DumpCmd.Flags().StringVarP(
		&dbUser,
		"username",
		"U",
		"",
		"Database username",
	)
	_ = viper.BindPFlag("dump.username", DumpCmd.Flags().Lookup("username"))

}

// cliCommandDump verifies that the supplied configuration is correct and starts the Dump process. If there is an error
// this function will notify others using the slack URI supplied in the configuration.
func cliCommandDump(cmd *cobra.Command, args []string) {
	var err error

	log.Info(aurora.Bold(aurora.Yellow(fmt.Sprint("Enabling log level: ",
		strings.ToUpper(viper.GetString("log-level"))))),
	)

	// Check to make sure schema-prefix and schema are used in conjunction with each other
	if len(viper.GetString("dump.schema-prefix")) > 0 && len(viper.GetStringSlice("dump.schema")) < 1 {
		log.Fatal("If a using --schema-prefix=schema_name_ you must add --schema=schema_name as well")
	}

	if len(viper.GetStringSlice("dump.exclude-table")) > 0 {
		log.Info(aurora.Bold(aurora.Magenta("Excluding the following tables:")))
		for _, t := range viper.GetStringSlice("dump.exclude-table") {
			log.Info("\t\t", t)
		}
	}

	if len(viper.GetStringSlice("dump.exclude-table-data")) > 0 {
		log.Info(aurora.Bold(aurora.Magenta("Excluding data from the following tables:")))
		for _, t := range viper.GetStringSlice("dump.exclude-table-data") {
			log.Info("\t\t", t)
		}
	}

	// If no password was supplied grab from user input
	if len(viper.GetString("dump.password")) < 1 {
		log.Debug("Password is empty. Asking user for password")
		viper.SetDefault("dump.password", GetPassword())
	}

	dbConf, _ := GetDb(
		viper.GetString("dump.host"),
		viper.GetString("dump.username"),
		viper.GetString("dump.password"),
		viper.GetString("dump.database"),
		viper.GetInt32("dump.port"),
		viper.GetBool("dump.disable-ssl"),
	)

	// Check to see if we need to complete row counts at the end of the dump process
	if len(viper.GetString("dump.row-count-file")) > 1 {
		excludeAllTables := append(
			viper.GetStringSlice("dump.exclude-table"),
			viper.GetStringSlice("dump.exclude-table-data")...,
		)
		log.Info("Storing table row counts CSV to: ", viper.GetString("dump.row-count-file"))
		err = storeRowCountFile(
			dbConf,
			viper.GetString("dump.schema-prefix"),
			viper.GetString("dump.row-count-file"),
			excludeAllTables)
		if err != nil {
			log.Error(err)
			log.Error("❌ Gonymizer did not exit properly. See above for errors ❌")
			os.Exit(1)
		}
	}

	log.Info("🚜 ", aurora.Bold(aurora.Green("Creating dump file")), " 🚜")
	err = dump(
		dbConf,
		viper.GetString("dump.dump-file"),
		viper.GetString("dump.schema-prefix"),
		viper.GetStringSlice("dump.exclude-table"),
		viper.GetStringSlice("dump.exclude-table-data"),
		viper.GetStringSlice("dump.exclude-schemas"),
		viper.GetStringSlice("dump.schema"),
	)

	if err != nil {
		log.Error(err)
		log.Error("❌ Gonymizer did not exit properly. See above for errors ❌")
		os.Exit(1)
	} else {
		log.Info("🦄 ", aurora.Bold(aurora.Green("-- SUCCESS --")), " 🌈")
	}
}

// dump initiates the dump process.
func dump(
	conf gonymizer.PGConfig,
	dumpFile,
	schemaPrefix string,
	excludeTable,
	excludeTableData,
	excludeSchemas,
	schema []string,
) (err error) {
	return gonymizer.CreateDumpFile(
		conf,
		dumpFile,
		schemaPrefix,
		excludeTable,
		excludeTableData,
		excludeSchemas,
		schema,
	)
}

// storeRowCountFile stores the row counts for every table that was saved into the dump file. This can be used during
// the load process to verify that all the included tables were anonymized and transferred properly.
func storeRowCountFile(dbConf gonymizer.PGConfig, schemaPrefix, path string, excludeTable []string) (err error) {
	rowCounts, err := gonymizer.GetTableRowCountsInDB(dbConf, schemaPrefix, excludeTable)
	if err != nil {
		return err
	}

	if len(*rowCounts) < 1 {
		return errors.New("We received 0 row counts for database: " + dbConf.DefaultDBName)
	}

	// S3 Storage
	if strings.HasPrefix(strings.ToLower(path), "s3://") {
		var s3file gonymizer.S3File
		log.Debug("S3 path detected for row-count-file")
		if err := s3file.ParseS3Url(path); err != nil {
			return err
		}

		// NOTE: Temp file creation. We will remove below after we load to S3
		// Use /tmp/filePath_EPOCH_TIME to store the file before copying to S3
		tempFile := "/tmp/" + strconv.FormatInt(time.Now().Unix(), 10) + "_" + s3file.FilePath
		log.Debug("Creating temp file: ", tempFile)
		f, err := os.OpenFile(tempFile, os.O_RDWR|os.O_CREATE, 0644)

		if err != nil {
			return err
		}
		defer f.Close()
		if err = saveToCsv(f, rowCounts); err != nil {
			return err
		}

		log.Infof("🚛 Uploading '%s' => S3: %s\n", tempFile, s3file.URL)
		sess, err := session.NewSession(&aws.Config{Region: aws.String(s3file.Region)})
		if err != nil {
			return err
		}

		if err = gonymizer.AddFileToS3(sess, tempFile, &s3file); err != nil {
			log.Errorf("Unable to upload '%s' => '%s'", tempFile, s3file.URL)
			return err
		}

		// Local Storage
	} else {
		log.Debug("Saving row-count-file to local disk: ", path)
		f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)

		if err != nil {
			return err
		}
		defer f.Close()

		if err = saveToCsv(f, rowCounts); err != nil {
			return err
		}
	}
	return nil
}

// saveToCsv saves the rowCounts file to the specified location that the file pointer is pointing to.
func saveToCsv(writer *os.File, rowCounts *[]gonymizer.RowCounts) (err error) {
	// Write table row counts to file
	csvWriter := csv.NewWriter(writer)
	for _, row := range *rowCounts {
		if err = csvWriter.Write([]string{*row.SchemaName, *row.TableName, strconv.Itoa(*row.Count)}); err != nil {
			return err
		}
	}
	csvWriter.Flush()
	return err
}
