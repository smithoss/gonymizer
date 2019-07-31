package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	log "github.com/sirupsen/logrus"

	"github.com/logrusorgru/aurora"
	"github.com/smithoss/gonymizer"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// UploadCmd is the cobra.Command struct we use for "upload" command.
var (
	UploadCmd = &cobra.Command{
		Use:   "upload",
		Short: "Upload will transfer a file to the given s3 path",
		Run:   cliCommandUpload,
	}
)

// init initializes the Upload command for the application and adds application flags and options.
func init() {
	UploadCmd.Flags().StringVar(
		&s3File,
		"s3-file",
		"",
		"S3 URL to upload processed file to: s3://bucket-name.us-west-2.s3.amazonaws.com/path/to/file.sql",
	)
	_ = viper.BindPFlag("upload.s3-file", UploadCmd.Flags().Lookup("s3-file"))

	UploadCmd.Flags().StringVar(
		&localFile,
		"local-file",
		"",
		"Local file path of file to upload",
	)
	_ = viper.BindPFlag("upload.local-file", UploadCmd.Flags().Lookup("local-file"))

}

// ClICommandUpload is the initialization point for executing the Upload process from the CLI and returns to the CLI on exit.
func cliCommandUpload(cmd *cobra.Command, args []string) {
	log.Info(aurora.Bold(aurora.Yellow(fmt.Sprint("Enabling log level: ",
		strings.ToUpper(viper.GetString("log-level"))))))
	if err := upload(viper.GetString("upload.local-file"), viper.GetString("upload.s3-file")); err != nil {
		log.Error("❌ Gonymizer did not exit properly. See above for errors ❌")
		os.Exit(1)
	} else {
		log.Info("🦄 ", aurora.Bold(aurora.Green("-- SUCCESS --")), " 🌈")
	}
}

func upload(localFile, urlStr string) (err error) {
	var s3file gonymizer.S3File

	log.Debug("s3 path: ", urlStr)
	if err = s3file.ParseS3Url(urlStr); err != nil {
		log.Error(err)
	}
	log.Debugf("S3 URL: %s\tScheme: %s\tBucket: %s\tRegion: %s\tFile Path: %s",
		s3file.URL,
		s3file.Scheme,
		s3file.Bucket,
		s3file.Region,
		s3file.FilePath,
	)

	// Upload the processed file to S3 (iff the user selected this option and it is valid)
	log.Info("S3 scheme: ", s3file.Scheme)
	switch s3file.Scheme {
	case "s3":
		log.Infof("🚛 Uploading %s => %s", localFile, s3file.URL)
		sess, err := session.NewSession(&aws.Config{Region: aws.String(s3file.Region)})
		if err != nil {
			return err
		}

		if err = gonymizer.AddFileToS3(sess, localFile, &s3file); err != nil {
			log.Errorf("Unable to upload %s => %s", localFile, s3file.URL)
			return err
		}

		//TODO: Add other cloud service providers here
	default:
		log.Error("Unrecognized transfer protocol: ", s3file.Scheme)
	}
	return err
}
