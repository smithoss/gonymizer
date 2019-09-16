package main

import (
	"fmt"
	"os"
	"strings"

	log "github.com/sirupsen/logrus"

	"github.com/logrusorgru/aurora"
	"github.com/smithoss/gonymizer"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	processedFile string

	// ProcessCmd is the cobra.Command struct we use for the "process" command.
	ProcessCmd = &cobra.Command{
		Use:   "process",
		Short: "Process will use the map file to anonymize data from a PostgreSQL dump file",
		Run:   cliCommandProcess,
	}
)

// init initializes the dump command for the application and adds application flags and options.
func init() {
	ProcessCmd.Flags().BoolVar(
		&generateSeed,
		"generate-seed",
		false,
		"Use Go's crypto package to generate seed values (instead of map file) for processors that require randomness",
	)
	_ = viper.BindPFlag("process.generate-seed", ProcessCmd.Flags().Lookup("generate-seed"))

	ProcessCmd.Flags().StringVar(
		&mapFile,
		"map-file",
		"",
		"Map file location",
	)
	_ = viper.BindPFlag("process.map-file", ProcessCmd.Flags().Lookup("map-file"))

	ProcessCmd.Flags().StringVar(
		&dumpFile,
		"dump-file",
		"",
		"Filename and location of the PII-PostgreSQL dump file",
	)
	_ = viper.BindPFlag("process.dump-file", ProcessCmd.Flags().Lookup("dump-file"))

	ProcessCmd.Flags().StringVar(
		&processedFile,
		"processed-file",
		"",
		"Filename and location to store the non-PII processed PostgreSQL dump file",
	)
	_ = viper.BindPFlag("process.processed-file", ProcessCmd.Flags().Lookup("processed-file"))

	ProcessCmd.Flags().StringVar(
		&preProcessFile,
		"pre-process-file",
		"",
		"SQL File to concatenate to the end of the processed dump file. Useful for adding static credentials to the database",
	)

	_ = viper.BindPFlag("process.pre-process-file", ProcessCmd.Flags().Lookup("pre-process-file"))
	ProcessCmd.Flags().StringVar(
		&postProcessFile,
		"post-process-file",
		"",
		"SQL File to prepend to the processed dump file. Useful for importing plugins",
	)
	_ = viper.BindPFlag("process.post-process-file", ProcessCmd.Flags().Lookup("post-process-file"))

}

// ClICommandProcess is the initialization point for executing the Process command from the CLI and returns to the CLI
// on exit. If there is an error this function will notify others using the slack URI supplied in the configuration.
func cliCommandProcess(cmd *cobra.Command, args []string) {
	var err error

	log.Info(aurora.Bold(aurora.Yellow(fmt.Sprint("Enabling log level: ",
		strings.ToUpper(viper.GetString("log-level"))))))

	log.Info("🚜 ", aurora.Bold(aurora.Green("Processing dump file")), " 🚜")
	err = process(
		viper.GetString("process.dump-file"),
		viper.GetString("process.map-file"),
		viper.GetString("process.processed-file"),
		viper.GetString("process.pre-process-file"),
		viper.GetString("process.post-process-file"),
		viper.GetBool("process.generate-seed"),
	)
	if err != nil {
		log.Error(err)
		log.Error("❌ Gonymizer did not exit properly. See above for errors ❌")
		os.Exit(1)
	} else {
		log.Info("🦄 ", aurora.Bold(aurora.Green("-- SUCCESS --")), " 🌈")
	}
}

// process is the entry point for processing a dump file according to the map file.
func process(dumpFile, mapFile, processedDumpFile, preProcess, postProcess string, generateSeed bool) (err error) {
	log.Info("Loading map file from: ", mapFile)
	columnMap, err := gonymizer.LoadConfigSkeleton(mapFile)
	if err != nil {
		return err
	}

	log.Info("Processing dump file: ", dumpFile)
	err = gonymizer.ProcessDumpFile(columnMap, dumpFile, processedDumpFile, preProcess,
		postProcess, generateSeed)
	if err != nil {
		return err
	}

	return nil
}
