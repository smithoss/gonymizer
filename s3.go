package gonymizer

import (
	"bytes"
	"errors"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	log "github.com/sirupsen/logrus"
	"net/http"
	"net/url"
	"os"
	"strings"
)

// S3File is the main structure for gonymizer files in S3 metadata.
type S3File struct {
	Bucket   string
	FilePath string
	Region   string
	Scheme   string
	Url      *url.URL
}


// ParseS3Url will parse the supplied S3 uri and load it into a S3File structure
func (this *S3File) ParseS3Url(s3url string) (err error) {
	// Parse S3 URL into Bucket, Region, and path
	if s3url != "" {
		this.Url, err = url.Parse(s3url)
		if err != nil {
			log.Error("Unable to parse URL string: ", s3url)
			return err
		}

		// We need to split up the URL for the host string to pull out Bucket and Region
		// Structure: <Bucket>.s3.<Region>.amazonaws.com<path>
		hostSplit := strings.Split(this.Url.Host, ".")
		this.Scheme = strings.Split(s3url, ":")[0]
		this.Bucket = hostSplit[0]
		this.Region = hostSplit[2]
		this.FilePath = this.Url.Path[1:] // Chop the first / from the path
		if this.Url.Scheme != "s3" {
			return errors.New("Unable to parse S3File URL: " + s3url)
		}
		log.Debugf("ParseS3Url => Bucket: %s\tRegion: %s\tFilePath: %s", this.Bucket, this.Region, this.FilePath)
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

	file, err := os.Open(inFile)
	if err != nil {
		return err
	}
	defer file.Close()

	// Get file size and read the file content into a buffer
	fileInfo, err := file.Stat()
	if err != nil {
		log.Error("Unable to get file stats: ", inFile)
		return err
	}

	size := fileInfo.Size()
	buffer := make([]byte, size)
	_, err = file.Read(buffer)
	if err != nil {
		return err
	}

	// Config settings: this is where you choose the Bucket, filename, content-type etc.
	// of the file you're uploading.
	_, err = s3.New(sess).PutObject(&s3.PutObjectInput{
		Bucket:               aws.String(s3file.Bucket),
		Key:                  aws.String(s3file.FilePath),
		ACL:                  aws.String("private"),
		Body:                 bytes.NewReader(buffer),
		ContentLength:        aws.Int64(size),
		ContentType:          aws.String(http.DetectContentType(buffer)),
		ContentDisposition:   aws.String("attachment"),
		ServerSideEncryption: aws.String("AES256"),
	})
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
		log.Errorf("Unable to download item: %s", s3file.Url.String())
		return err
	}
	return nil
}
