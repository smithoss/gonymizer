package main

import (
	"database/sql"
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"
	"syscall"

	log "github.com/sirupsen/logrus"
	prefixed "github.com/x-cray/logrus-prefixed-formatter"
	"golang.org/x/crypto/ssh/terminal"

	"github.com/logrusorgru/aurora"
	"github.com/smithoss/gonymizer"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const longHelp = `
Command usage:

1. create a map file: ./gonymizer map
2. create a pii-encumbered dump file: ./gonymizer dump
3. process pii dump file and create an altered dump file: ./gonymizer process


gonymizer command arguments:
		./gonymizer map --help

gonymizer map examples:

    ./gonymizer map
    ./gonymizer -c staging.json --map-file=map.json --schema="db_*" map

gonymizer dump examples:

    ./gonymizer -c staging.json --map-file=map.json --schema="db_*" --dump-file=pii.sql dump

gonymizer process examples:

    ./gonymizer -c config.yaml --dump-file=pii.sql --processed-dumpfile=anonymized.sql process


License Information
	https://raw.githubusercontent.com/smithoss/gonymizer/master/LICENSE.txt

`

var (
	configPath       string
	dbUser           string
	dbHost           string
	dbName           string
	dbPassword       string
	dbPort           int32
	dbDisableSSL     bool
	excludeSchemas   []string
	excludeTable     []string
	excludeTableData []string
	generateSeed     bool
	loadFile         string
	logFile          string
	logFormat        string
	logLevel         string
	mapFile          string
	dumpFile         string
	postProcessFile  string
	procedures       bool
	rowCountFile     string
	schemaPrefix     string
	s3FilePath       string
	schema           []string

	rootCmd = &cobra.Command{
		Use:              "gonymizer",
		Short:            "Usage: gonymizer [optional_flags] map|dump|process|load",
		Long:             longHelp,
		PersistentPreRun: preRun,
	}
)

// GetDb returns a PGConfig set to the supplied database settings.
func GetDb(host, username, password, database string, port int32, disableSSL bool) (gonymizer.PGConfig, *sql.DB) {
	conf := gonymizer.PGConfig{}
	conf.LoadFromCLI(host, username, password, database, port, disableSSL)

	db, err := gonymizer.OpenDB(conf)
	if err != nil {
		log.Fatal(err)
	}

	err = db.Ping()
	if err != nil {
		log.Fatal(err)
	}

	return conf, db
}

// GetPassword will ask the user to input a database password from the CLI if the password was left blank in the
// configuration. Returns the password as a string.
func GetPassword() string {
	fmt.Print("Database Password: ")
	bytePassword, err := terminal.ReadPassword(int(syscall.Stdin))
	fmt.Println() // terminal.ReadPassword does not add a new line after receiving the password
	if err != nil {
		log.Error("Unable to read password")
		os.Exit(1)
	}
	return string(bytePassword)
}

// Execute executes the root command.
func main() {
	if err := rootCmd.Execute(); err != nil {
		log.Fatal(err)
		os.Exit(1)
	}
}

// init initializes the persistent flags for all commands for the application.
func init() {
	cobra.OnInitialize(initConfig)

	Formatter := new(log.TextFormatter)
	Formatter.TimestampFormat = "2006-01-02 15:04:05.000"
	Formatter.FullTimestamp = true
	log.SetFormatter(Formatter)

	rootCmd.PersistentFlags().StringVarP(
		&configPath,
		"config",
		"c",
		"",
		"Path to configuration file (types: TOML, YAML, JSON)",
	)

	rootCmd.PersistentFlags().StringVarP(
		&logFile,
		"log-file",
		"l",
		"/tmp/gonymizer.log",
		"If log-type=file, the /path/to/logfile; ignored otherwise",
	)
	_ = viper.BindPFlag("log.file", rootCmd.PersistentFlags().Lookup("log-file"))

	rootCmd.PersistentFlags().StringVarP(
		&logLevel,
		"log-level",
		"L",
		"info",
		"Output level of logs (TRACE, DEBUG, INFO, WARN, ERROR, FATAL)",
	)
	_ = viper.BindPFlag("log.level", rootCmd.PersistentFlags().Lookup("log-level"))

	rootCmd.PersistentFlags().StringVarP(
		&logFormat,
		"log-format",
		"f",
		"clean",
		"Type of output, one of: json, text, clean",
	)
	_ = viper.BindPFlag("log.format", rootCmd.PersistentFlags().Lookup("log-format"))

	// Bind commands to root
	rootCmd.AddCommand(
		DumpCmd,
		LoadCmd,
		MapCmd,
		ProcessCmd,
		VersionCmd,
	)
}

// initConfig will load the configuration file (if supplied), and then load the ENV variables, and finally use the
// CLI flags to setup the configuration before running the application.
func initConfig() {
	// 1. Load config file
	if configPath != "" {
		// Use config file from the flag.
		viper.SetConfigFile(configPath)
	}

	if err := viper.ReadInConfig(); err == nil {
		fmt.Println("Using config file:", viper.ConfigFileUsed())
	}

	// 2. Load ENV variables
	viper.SetEnvPrefix("GON")
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.AutomaticEnv()

	// 3. Load flags/cli-args into Viper from Cobra
	if err := viper.BindPFlags(DumpCmd.Flags()); err != nil {
		log.Error("Unable to bind flags")
	}
	if err := viper.BindPFlags(MapCmd.Flags()); err != nil {
		log.Error("Unable to bind flags")
	}
	if err := viper.BindPFlags(ProcessCmd.Flags()); err != nil {
		log.Error("Unable to bind flags")
	}
	if err := viper.BindPFlags(rootCmd.PersistentFlags()); err != nil {
		log.Error("Unable to bind flags")
	}
}

// preRun sets up default logging as well as printing the build number and date to the screen for debug purposes.
func preRun(cmd *cobra.Command, args []string) {
	setupLogging()

	log.Infof("Starting %v (v%v, build %v, build date: %v)",
		os.Args[0],
		gonymizer.Version(),
		gonymizer.BuildNumber(),
		gonymizer.BuildDate(),
	)

	log.Infof("Go (runtime: %v) (GOMAXPROCS: %d) (NumCPUs: %d)\n",
		runtime.Version(),
		runtime.GOMAXPROCS(-1),
		runtime.NumCPU(),
	)
}

// setLoggingLevel sets the logging level depending on the application configuration.
func setupLogging() {
	// Setup Log File
	if viper.GetString("log-file") != "" {
		if f, err := os.OpenFile(viper.GetString("log-file"), os.O_WRONLY|os.O_CREATE, 0755); err != nil {
			os.Exit(1)
		} else {
			// Write to stdout and the file
			mw := io.MultiWriter(os.Stdout, f)
			log.SetOutput(mw)
		}
	}

	// Setup Log Formatter
	switch strings.ToLower(viper.GetString("log.format")) {
	case "json":
		log.SetFormatter(&log.JSONFormatter{})
	case "text":
		log.SetFormatter(&log.TextFormatter{})
	default:
		log.SetFormatter(&prefixed.TextFormatter{})
	}

	// Setup Log level
	switch strings.ToLower(viper.GetString("log.level")) {
	case "debug":
		log.SetLevel(log.DebugLevel)
	case "info":
		log.SetLevel(log.InfoLevel)
	case "error":
		log.SetLevel(log.ErrorLevel)
	case "fatal":
		log.SetLevel(log.FatalLevel)
	case "warn":
		log.SetLevel(log.WarnLevel)
	default:
		log.SetLevel(log.InfoLevel)
	}

	if log.GetLevel() == log.DebugLevel {
		log.Debugf("os.Args: %v", os.Args)
		log.Debugf("🐍 %s 👇", aurora.Bold(aurora.Green(fmt.Sprintf(" configuration "))))
		viper.Debug()
		log.Debugf("🐍 %s ☝️", aurora.Bold(aurora.Green(fmt.Sprintf(" configuration "))))
	}
}
