package gonymizer

import (
	"errors"
	"net/url"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	log "github.com/sirupsen/logrus"
)

// S3File is the main structure for gonymizer files in S3 metadata.
type S3File struct {
	Bucket   string
	FilePath string
	Region   string
	Scheme   string
	URL      *url.URL
}

// ParseS3Url will parse the supplied S3 uri and load it into a S3File structure
func (s3f *S3File) ParseS3Url(s3url string) (err error) {
	// Parse S3 URL into Bucket, Region, and path
	if s3url != "" {
		s3f.URL, err = url.Parse(s3url)
		if err != nil {
			log.Error("Unable to parse URL string: ", s3url)
			return err
		}

		// We need to split up the URL for the host string to pull out Bucket and Region
		// Structure: <Bucket>.s3.<Region>.amazonaws.com<path>
		hostSplit := strings.Split(s3f.URL.Host, ".")
		s3f.Scheme = strings.Split(s3url, ":")[0]
		s3f.Bucket = hostSplit[0]
		s3f.Region = hostSplit[2]
		s3f.FilePath = s3f.URL.Path[1:] // Chop the first / from the path
		if s3f.URL.Scheme != "s3" {
			return errors.New("Unable to parse S3File URL: " + s3url)
		}
		log.Debugf("ParseS3Url => Bucket: %s\tRegion: %s\tFilePath: %s", s3f.Bucket, s3f.Region, s3f.FilePath)
	}
	return nil
}

// AddFileToS3 will upload the supplied inFile to the supplied S3File.FilePath
func AddFileToS3(sess *session.Session, inFile string, s3file *S3File) (err error) {
	if sess == nil {
		sess, err = session.NewSession(&aws.Config{Region: aws.String(s3file.Region)})
		if err != nil {
			return err
		}
	}

	// Get file stats
	file, err := os.Open(inFile)
	if err != nil {
		return err
	}
	defer file.Close()

	// Get file size
	fileInfo, err := file.Stat()
	if err != nil {
		log.Error("Unable to get file stats: ", inFile)
		return err
	}
	fileSize := fileInfo.Size()
	log.Infof("File Size: %.2f GB", (float64(fileSize) / float64(1024*1024*1024)))

	// Use s3 manager to upload the file in pieces
	//select Region to use.
	svc := s3manager.NewUploader(sess)

	response, err := svc.Upload(&s3manager.UploadInput{
		ACL:                  aws.String("private"),
		Body:                 file,
		Bucket:               aws.String(s3file.Bucket),
		ContentDisposition:   aws.String("attachment"),
		ContentType:          aws.String("text/plain"),
		Key:                  aws.String(s3file.FilePath),
		ServerSideEncryption: aws.String("AES256"),
	})
	log.Debug("AWS Response: ", response)
	return err
}

// GetFileFromS3 will save the S3File to the loadFile destination.
func GetFileFromS3(sess *session.Session, s3file *S3File, loadFile string) (err error) {
	// Download the file to the loadFile destination
	if sess == nil {
		sess, err = session.NewSession(&aws.Config{Region: aws.String(s3file.Region)})
		if err != nil {
			return err
		}
	}
	file, err := os.OpenFile(loadFile, os.O_RDWR|os.O_CREATE, 0660)
	if err != nil {
		return err
	}
	defer file.Close()

	downloader := s3manager.NewDownloader(sess)
	_, err = downloader.Download(
		file,
		&s3.GetObjectInput{
			Bucket: aws.String(s3file.Bucket),
			Key:    aws.String(s3file.FilePath),
		})
	if err != nil {
		log.Errorf("Unable to download item: %s", s3file.URL.String())
		return err
	}
	return nil
}
