package main

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"os"
	"strings"

	"github.com/logrusorgru/aurora"
	"github.com/smithoss/gonymizer"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// MapCmd is the cobra.Command struct we use for "map" command.
var (
	MapCmd = &cobra.Command{
		Use:   "map",
		Short: "Map creates/modifies the map file for a PostgreSQL database",
		Run:   cliCommandMap,
	}
)

// init initializes the Map command for the application and adds application flags and options.
func init() {

	MapCmd.Flags().BoolVarP(
		&dbDisableSSL,
		"disable-ssl",
		"S",
		false,
		"Disable SSL (Not-recommended)",
	)
	_ = viper.BindPFlag("map.disable-ssl", MapCmd.Flags().Lookup("disable-ssl"))

	MapCmd.Flags().StringVarP(
		&mapFile,
		"map-file",
		"m",
		"",
		"Map file location",
	)
	_ = viper.BindPFlag("map.map-file", MapCmd.Flags().Lookup("map-file"))

	MapCmd.Flags().StringSliceVar(
		&excludeTable,
		"exclude-table",
		[]string{},
		"A table, or list of tables, that do not contain data or are not included in the dump file",
	)
	_ = viper.BindPFlag("map.exclude-table", MapCmd.Flags().Lookup("exclude-table"))

	MapCmd.Flags().StringSliceVar(
		&excludeTableData,
		"exclude-table-data",
		[]string{},
		"A table's data, or list of tables' data, that we do not want to include data in the dump "+
			"(--exclude-table-data in pg_dump)",
	)
	_ = viper.BindPFlag("map.exclude-table-data", MapCmd.Flags().Lookup("exclude-table-data"))

	MapCmd.Flags().StringVarP(
		&dbHost,
		"host",
		"H",
		"",
		"Database host address",
	)
	_ = viper.BindPFlag("map.host", MapCmd.Flags().Lookup("host"))

	MapCmd.Flags().StringVarP(
		&dbName,
		"database",
		"d",
		"",
		"Database name",
	)
	_ = viper.BindPFlag("map.database", MapCmd.Flags().Lookup("database"))

	MapCmd.Flags().StringSliceVar(
		&schema,
		"schema",
		[]string{},
		"Schema to dump to the SQL file (can use more than one)",
	)
	_ = viper.BindPFlag("map.schema", MapCmd.Flags().Lookup("schema"))

	MapCmd.Flags().StringVar(
		&schemaPrefix,
		"schema-prefix",
		"",
		"The schema prefix for grouped schemas. I.E. --schema-prefix=mdb_ would match all 'mdb_*' "+
			"schemas in the catalog",
	)
	_ = viper.BindPFlag("map.schema-prefix", MapCmd.Flags().Lookup("schema-prefix"))

	MapCmd.Flags().StringVarP(
		&dbPassword,
		"password",
		"p",
		"",
		"Database password",
	)
	_ = viper.BindPFlag("map.password", MapCmd.Flags().Lookup("password"))

	MapCmd.Flags().Int32VarP(
		&dbPort,
		"port",
		"P",
		5432,
		"Database port",
	)
	_ = viper.BindPFlag("map.port", MapCmd.Flags().Lookup("port"))

	MapCmd.Flags().StringVarP(
		&dbUser,
		"username",
		"U",
		"",
		"Database username",
	)
	_ = viper.BindPFlag("map.username", MapCmd.Flags().Lookup("username"))

}

// ClICommandMap is the initialization point for executing the Map process from the CLI and returns to the CLI on exit.
func cliCommandMap(cmd *cobra.Command, args []string) {
	var err error

	log.Info(aurora.Bold(aurora.Brown(fmt.Sprint("Enabling log level: ",
		strings.ToUpper(viper.GetString("log-level"))))))

	// Check to make sure schema-prefix and schema are used in conjunction with each other
	if len(viper.GetString("map.schema-prefix")) > 0 && len(viper.GetStringSlice("map.schema")) < 1 {
		log.Fatal("If a using --schema-prefix=schema_name_ you must add --schema=schema_name as well")
	}

	if len(viper.GetStringSlice("map.exclude-table")) > 0 {
		log.Info(aurora.Bold(aurora.Magenta("Excluding the following tables:")))
		for _, t := range viper.GetStringSlice("map.exclude-table") {
			log.Info("\t\t", t)
		}
	}

	if len(viper.GetStringSlice("map.exclude-table-data")) > 0 {
		log.Info(aurora.Bold(aurora.Magenta("Excluding data from the following tables:")))
		for _, t := range viper.GetStringSlice("map.exclude-table-data") {
			log.Info("\t\t", t)
		}
	}

	if len(viper.GetString("map.password")) < 1 {
		log.Debug("Password is empty. Asking user for password")
		viper.SetDefault("map.password", GetPassword())
	}

	dbConf, _ := GetDb(
		viper.GetString("map.host"),
		viper.GetString("map.username"),
		viper.GetString("map.password"),
		viper.GetString("map.database"),
		viper.GetInt32("map.port"),
		viper.GetBool("map.disable-ssl"),
	)
	err = runMap(
		dbConf,
		viper.GetString("map.map-file"),
		viper.GetString("map.schema-prefix"),
		viper.GetStringSlice("map.exclude-table"),
		viper.GetStringSlice("map.exclude-table-data"),
		viper.GetStringSlice("map.schema"),
	)
	if err != nil {
		log.Error(err)
		log.Error("❌ Gonymizer did not exit properly. See above for errors ❌")
		os.Exit(1)
	} else {
		log.Info("🦄 ", aurora.Bold(aurora.Green("-- SUCCESS --")), " 🌈")
	}
}

// runMap will map the database and update a configuration skeleton or create a new configuration skeleton.
func runMap(
	conf gonymizer.PGConfig,
	mapFile,
	schemaPrefix string,
	excludeTable,
	excludeTableData,
	schema []string,
) (err error) {
	var (
		skeleton *gonymizer.DBMapper
	)

	// Concatenate lists since we do not care for mapping sake
	excludeTablesLocal := append(excludeTable, excludeTableData...)

	if len(excludeTablesLocal) > 0 {
		log.Info(aurora.Bold(aurora.Magenta("Excluding the following tables:")))
		for _, t := range excludeTablesLocal {
			log.Info("\t\t", t)
		}
	}

	log.Info("🚜 ", aurora.Bold(aurora.Green("Creating map file")), " 🚜")
	skeleton, err = gonymizer.GenerateConfigSkeleton(
		conf,
		schemaPrefix,
		schema,
		excludeTablesLocal,
	)
	if err != nil {
		return err
	}

	skeletonFile := fmt.Sprint(mapFile + ".skeleton.json")
	err = gonymizer.WriteConfigSkeleton(skeleton, skeletonFile)
	if err != nil {
		return err
	}

	log.Info("Wrote skeleton file to: ", skeletonFile)
	log.Info("Found ", len(skeleton.ColumnMaps), " columns in ", skeleton.DBName, ".public")

	return nil

}
