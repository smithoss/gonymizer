package main

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
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
		&postProcessFile,
		"post-process-file",
		"",
		"File to concatenate to the end of the processed dump file. Useful for adding static credentials to the database",
	)
	_ = viper.BindPFlag("process.map-file", ProcessCmd.Flags().Lookup("post-process-file"))

	ProcessCmd.Flags().StringVar(
		&s3FilePath,
		"s3-file-path",
		"",
		"S3 URL to upload processed file to: s3://bucket-name.us-west-2.s3.amazonaws.com/path/to/file.txt",
	)
	_ = viper.BindPFlag("process.s3-file-path", ProcessCmd.Flags().Lookup("s3-file-path"))

}

// ClICommandProcess is the initialization point for executing the Process command from the CLI and returns to the CLI
// on exit. If there is an error this function will notify others using the slack URI supplied in the configuration.
func cliCommandProcess(cmd *cobra.Command, args []string) {
	var (
		s3file gonymizer.S3File
		err    error
	)

	log.Info(aurora.Bold(aurora.Brown(fmt.Sprint("Enabling log level: ",
		strings.ToUpper(viper.GetString("log-level"))))))

	// Parse S3 URL into bucket, region, and path
	urlStr := viper.GetString("process.s3-file-path")
	log.Debug("s3-file-path: ", urlStr)

	if err = s3file.ParseS3Url(urlStr); err != nil {
		log.Error(err)
		log.Error("❌ Gonymizer did not exit properly. See above for errors ❌")
		os.Exit(1)
	}
	log.Debugf("S3 URL: %s\tScheme: %s\tBucket: %s\tRegion: %s\tFile Path: %s",
		s3file.URL,
		s3file.Scheme,
		s3file.Bucket,
		s3file.Region,
		s3file.FilePath,
	)

	log.Info("🚜 ", aurora.Bold(aurora.Green("Processing dump file")), " 🚜")
	err = process(
		viper.GetString("process.dump-file"),
		viper.GetString("process.map-file"),
		viper.GetString("process.processed-file"),
		viper.GetBool("process.generate-seed"),
		&s3file,
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
func process(dumpFile, mapFile, processedDumpFile string, generateSeed bool, s3file *gonymizer.S3File) (err error) {
	log.Info("Loading map file from: ", mapFile)
	columnMap, err := gonymizer.LoadConfigSkeleton(mapFile)
	if err != nil {
		return err
	}

	log.Info("Processing dump file: ", dumpFile)
	err = gonymizer.ProcessDumpFile(columnMap, dumpFile, processedDumpFile, postProcessFile, generateSeed)
	if err != nil {
		return err
	}

	// Upload the processed file to S3 (iff the user selected this option and it is valid)
	log.Info("S3 scheme: ", s3file.Scheme)
	if s3file.Scheme == "s3" {
		log.Infof("🚛 Uploading '%s' => S3: %s\n", processedDumpFile, s3file.URL)
		sess, err := session.NewSession(&aws.Config{Region: aws.String(s3file.Region)})
		if err != nil {
			return err
		}

		if err = gonymizer.AddFileToS3(sess, processedDumpFile, s3file); err != nil {
			log.Errorf("Unable to upload '%s' => '%s'", processedDumpFile, s3file.URL)
			return err
		}
	}
	return nil
}
