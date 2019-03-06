package main

import (
	"fmt"
	"github.com/smithoss/gonymizer"
	"github.com/logrusorgru/aurora"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"os"
	"strconv"
	"strings"
	"time"
)

var (
	LoadCmd = &cobra.Command{
		Use:   "load",
		Short: "Load an anonymized dump file into a PostgreSQL database",
		Run:   cliCommandLoad,
	}
)

// init initializes the Load command for the application and adds application flags and options.
func init() {

	LoadCmd.Flags().BoolVarP(
		&dbDisableSSL,
		"disable-ssl",
		"S",
		false,
		"Disable SSL (Not-recommended)",
	)
	_ = viper.BindPFlag("load.disable-ssl", LoadCmd.Flags().Lookup("disable-ssl"))

	LoadCmd.Flags().StringVarP(
		&dbHost,
		"host",
		"H",
		"",
		"Database host address",
	)
	_ = viper.BindPFlag("load.host", LoadCmd.Flags().Lookup("host"))

	LoadCmd.Flags().StringVarP(
		&dbName,
		"database",
		"d",
		"",
		"Database name",
	)
	_ = viper.BindPFlag("load.database", LoadCmd.Flags().Lookup("database"))

	LoadCmd.Flags().StringVar(
		&loadFile,
		"load-file",
		"",
		"Location to load file containing anonymized data",
	)
	_ = viper.BindPFlag("load.load-file", LoadCmd.Flags().Lookup("load-file"))

	LoadCmd.Flags().BoolVar(
		&procedures,
		"skip-procedures",
		false,
		"Skip adding stored procedures to load file",
	)
	_ = viper.BindPFlag("load.skip-procedures", LoadCmd.Flags().Lookup("skip-procedures"))

	LoadCmd.Flags().StringVarP(
		&dbPassword,
		"password",
		"p",
		"",
		"Database password",
	)
	_ = viper.BindPFlag("load.password", LoadCmd.Flags().Lookup("password"))

	LoadCmd.Flags().Int32VarP(
		&dbPort,
		"port",
		"P",
		5432,
		"Database port",
	)
	_ = viper.BindPFlag("load.port", LoadCmd.Flags().Lookup("port"))

	LoadCmd.Flags().StringVar(
		&rowCountFile,
		"row-count-file",
		"",
		"CSV file to load and compare row counts to (see documentation)",
	)
	_ = viper.BindPFlag("dump.row-count-file", LoadCmd.Flags().Lookup("row-count-file"))

	LoadCmd.Flags().StringVarP(
		&dbUser,
		"username",
		"U",
		"",
		"Database username",
	)
	_ = viper.BindPFlag("load.username", LoadCmd.Flags().Lookup("username"))

}

// cliCommandLoad verifies that the supplied configuration is correct and starts the Load process. If there is an error
// this function will notify others using the slack URI supplied in the configuration.
func cliCommandLoad(cmd *cobra.Command, args []string) {
	var err error

	log.Info(aurora.Bold(aurora.Brown(fmt.Sprint("Enabling log level: ",
		strings.ToUpper(viper.GetString("log-level"))))),
	)

	// If no password was supplied grab from user input
	if len(viper.GetString("load.password")) < 1 {
		log.Debug("Password is empty. Asking user for password")
		viper.SetDefault("load.password", GetPassword())
	}

	dbConf, _ := GetDb(
		viper.GetString("load.host"),
		viper.GetString("load.username"),
		viper.GetString("load.password"),
		viper.GetString("load.database"),
		viper.GetInt32("load.port"),
		viper.GetBool("load.disable-ssl"),
	)

	// Start the loading process
	log.Info("🚜 ", aurora.Bold(aurora.Green("Loading the anonymized database")), " 🚜")
	if err = load(dbConf, viper.GetString("load.load-file"), viper.GetString("load.s3-file-path")); err != nil {
		log.Error(err)
		log.Error("❌ Anonymizer did not exit properly. See above for errors ❌")
		os.Exit(1)
	}

	// Store row counts
	if len(viper.GetString("load.row-count-file")) > 1 {
		log.Info("Loading row-counts CSV file from: ", viper.GetString("load.row-count-file"))
		err = downloadRowCountFile(dbConf, viper.GetString("load.row-count-file"))
		if err != nil {
			log.Error(err)
			log.Error("❌ Anonymizer did not exit properly. See above for errors ❌")
			os.Exit(1)
		}
	}

	log.Info("🦄 ", aurora.Bold(aurora.Green("-- SUCCESS --")), " 🌈")
}

// load starts the loading process.
func load(conf gonymizer.PGConfig, loadFile, s3FilePath string) (err error) {
	// Check for S3 file here. If it is defined we should download it to loadFile's path and then load it.
	if s3FilePath != "" {
		log.Infof("🚛 Downloading from S3 '%s' -> %s\n", s3FilePath, loadFile)
		anonFile := new(gonymizer.S3File)
		if err = anonFile.ParseS3Url(s3FilePath); err != nil {
			return err
		}
		if err = gonymizer.GetFileFromS3(nil, anonFile, loadFile); err != nil {
			return err
		}
	}

	log.Info("Loading data from file: ", loadFile)
	if err = gonymizer.LoadFile(conf, loadFile); err != nil {
		return err
	} else {
		return nil
	}
}

// downloadRowCountFile will download the row count file from S3 if needed and verify that the table row counts is
// correct.
func downloadRowCountFile(dbConf gonymizer.PGConfig, path string) (err error) {
	// S3 Storage
	if strings.HasPrefix(strings.ToLower(path), "s3://"){
		var s3File gonymizer.S3File
		if err = s3File.ParseS3Url(path); err != nil {
			return err
		}

		// NOTE: Temp file creation. We will remove below after we load to S3
		// Use /tmp/filePath_EPOCH_TIME to store the file before copying to S3
		tempFile := "/tmp/" + strconv.FormatInt(time.Now().Unix(), 10) + "_" + s3File.FilePath
		log.Infof("🚛 Downloading from S3 '%s' -> %s\n", s3File.Url, tempFile)
		if err = gonymizer.GetFileFromS3(nil, &s3File, tempFile); err != nil {
			return err
		}
		err = gonymizer.VerifyRowCount(dbConf, tempFile)

	// Local file storage
	} else {
		err = gonymizer.VerifyRowCount(dbConf, path)
	}
	return err
}
