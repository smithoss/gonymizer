package gonymizer

import (
	"bytes"
	"net/http"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	log "github.com/sirupsen/logrus"
)

/* The original version of this library was taken from:
https://github.com/apoorvam/aws-s3-multipart-upload/blob/master/aws-multipart-upload.go
Modified by: Levi Junkert
*/
const (
	maxPartSize = int64(512 * 1000000) // 512 MB
	maxRetries  = 3
)

func S3MultiPartUpload(inputFile string, s3f *S3File) error {
	svc := s3.New(session.New(), aws.NewConfig().WithRegion(s3f.Region))
	origFile, err := os.Open(inputFile)
	if err != nil {
		return err
	}
	defer origFile.Close()

	fileInfo, _ := origFile.Stat()
	size := fileInfo.Size()
	buffer := make([]byte, size)
	fileType := http.DetectContentType(buffer)
	origFile.Read(buffer)

	input := &s3.CreateMultipartUploadInput{
		Bucket:               aws.String(s3f.Bucket),
		Key:                  aws.String(s3f.FilePath),
		ContentType:          aws.String(fileType),
		ServerSideEncryption: aws.String("AES256"),
	}

	resp, err := svc.CreateMultipartUpload(input)
	if err != nil {
		return err
	}
	log.Info("⬆ Created multipart upload request ⬆")

	var curr, partLength int64
	var remaining = size
	var completedParts []*s3.CompletedPart
	partNumber := 1
	for curr = 0; remaining != 0; curr += partLength {
		if remaining < maxPartSize {
			partLength = remaining
		} else {
			partLength = maxPartSize
		}
		completedPart, err := uploadPart(svc, resp, buffer[curr:curr+partLength], partNumber)
		if err != nil {
			log.Error(err.Error())
			err := abortMultipartUpload(svc, resp)
			if err != nil {
				log.Error(err.Error())
			}
			return err
		}
		remaining -= partLength
		partNumber++
		completedParts = append(completedParts, completedPart)
	}

	completeResponse, err := completeMultipartUpload(svc, resp, completedParts)
	if err != nil {
		log.Error(err.Error())
		return err
	}
	log.Info("🕊 Successfully uploaded: %s 🕊\n", completeResponse.String())
	return err
}

func completeMultipartUpload(
	svc *s3.S3,
	resp *s3.CreateMultipartUploadOutput,
	completedParts []*s3.CompletedPart) (*s3.CompleteMultipartUploadOutput, error) {

	completeInput := &s3.CompleteMultipartUploadInput{
		Bucket:   resp.Bucket,
		Key:      resp.Key,
		UploadId: resp.UploadId,
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: completedParts,
		},
	}
	return svc.CompleteMultipartUpload(completeInput)
}

func uploadPart(
	svc *s3.S3,
	resp *s3.CreateMultipartUploadOutput,
	fileBytes []byte,
	partNumber int) (*s3.CompletedPart, error) {

	tryNum := 1
	partInput := &s3.UploadPartInput{
		Body:          bytes.NewReader(fileBytes),
		Bucket:        resp.Bucket,
		Key:           resp.Key,
		PartNumber:    aws.Int64(int64(partNumber)),
		UploadId:      resp.UploadId,
		ContentLength: aws.Int64(int64(len(fileBytes))),
	}

	for tryNum <= maxRetries {
		uploadResult, err := svc.UploadPart(partInput)
		if err != nil {
			if tryNum == maxRetries {
				if aerr, ok := err.(awserr.Error); ok {
					return nil, aerr
				}
				return nil, err
			}
			log.Warn("Retrying to upload part #%v\n", partNumber)
			tryNum++
		} else {
			log.Info("  🛸 Uploaded part #%v\n", partNumber)
			return &s3.CompletedPart{
				ETag:       uploadResult.ETag,
				PartNumber: aws.Int64(int64(partNumber)),
			}, nil
		}
	}
	return nil, nil
}

func abortMultipartUpload(svc *s3.S3, resp *s3.CreateMultipartUploadOutput) error {
	log.Warnf("👎 Aborting multipart upload for UploadId #%s 👎", *resp.UploadId)
	abortInput := &s3.AbortMultipartUploadInput{
		Bucket:   resp.Bucket,
		Key:      resp.Key,
		UploadId: resp.UploadId,
	}
	_, err := svc.AbortMultipartUpload(abortInput)
	return err
}
