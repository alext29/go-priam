package prium

import (
	"compress/gzip"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/golang/glog"
	"github.com/pkg/errors"
	"io"
	"os"
	"path"
	"strings"
)

// S3 object interfaces with AWS S3.
type S3 struct {
	config   *Config
	agent    *Agent
	svc      *s3.S3
	uploader *s3manager.Uploader
}

// NewS3 creates a new S3 object to interface with AWS S3.
func NewS3(config *Config, agent *Agent) *S3 {
	// create new session
	sess := session.New(&aws.Config{
		Region:      aws.String(config.AwsRegion),
		Credentials: credentials.NewStaticCredentials(config.AwsAccessKey, config.AwsSecretKey, ""),
	})
	return &S3{
		config:   config,
		agent:    agent,
		svc:      s3.New(sess),
		uploader: s3manager.NewUploader(sess),
	}
}

// UploadFiles uploads a list of files to AWS S3.
// TODO: retry upload if initial upload fails.
func (s *S3) UploadFiles(parent, timestamp, host string, files []string) error {
	glog.Infof("uploading files to s3...")
	for _, file := range files {
		key := s.getFileKey(parent, timestamp, host, file)
		glog.Infof("upload key: %s", key)

		// read bytes from file@host
		r, err := s.agent.ReadFile(host, file)
		if err != nil {
			return errors.Wrap(err, fmt.Sprintf("read %s:%s", host, file))
		}

		// gzip files before uploading
		reader, writer := io.Pipe()
		go func() {
			gw := gzip.NewWriter(writer)
			//r := bytes.NewReader(b)
			io.Copy(gw, r)
			gw.Close()
			writer.Close()
		}()

		// details of file to upload
		params := &s3manager.UploadInput{
			Bucket: aws.String(s.config.AwsBucket),
			Body:   reader,
			Key:    aws.String(key),
		}

		// upload file
		_, err = s.uploader.Upload(params)
		if err != nil {
			return errors.Wrap(err, fmt.Sprintf("upload %s:%s", host, file))
		}
	}
	return nil
}

// getFileKey creates a unique key for backup file that would be uploaded
// to AWS S3.
func (s *S3) getFileKey(parent, timestamp, host, file string) string {
	dir, base := path.Split(path.Clean(file))
	dir, _ = path.Split(path.Clean(dir))
	if !s.config.Incremental {
		dir, _ = path.Split(path.Clean(dir))
	}
	return fmt.Sprintf("/%s/%s/%s/%s/%s%s%s.gz",
		s.config.AwsBasePath, s.config.Keyspace, parent, timestamp, host, dir, base)
}

// downloadKeys downloads a list of keys from S3 to local machine.
func (s *S3) downloadKeys(keys []string, prefix string) (map[string]string, error) {
	glog.Infof("downloading %d keys", len(keys))
	files := make(map[string]string)
	for _, key := range keys {
		file, err := s.downloadKey(key, prefix)
		if err != nil {
			return nil, errors.Wrap(err, fmt.Sprintf("download %s", key))
		}
		files[key] = file
	}
	return files, nil
}

func (s *S3) downloadKey(key, prefix string) (string, error) {
	glog.V(2).Infof("download key: %s", key)
	fileName := strings.TrimSuffix(fmt.Sprintf("%s/%s", prefix, key), ".gz")
	params := &s3.GetObjectInput{
		Bucket: aws.String(s.config.AwsBucket),
		Key:    aws.String(key),
	}
	resp, err := s.svc.GetObject(params)
	if err != nil {
		return "", errors.Wrap(err, fmt.Sprintf("error downloading key: %s", key))
	}

	dir := path.Dir(fileName)
	err = os.MkdirAll(dir, os.ModeDir|os.ModePerm)
	if err != nil {
		return "", errors.Wrap(err, fmt.Sprintf("error creading dir %s", dir))
	}

	file, err := os.Create(fileName)
	if err != nil {
		return "", errors.Wrap(err, "error opening file")
	}

	reader, writer := io.Pipe()
	go func() {
		gr, err := gzip.NewReader(resp.Body)
		if err != nil {
			glog.Errorf("error creating new gzip reader")
			os.Exit(1)
		}
		io.Copy(writer, gr)
		gr.Close()
		writer.Close()
	}()

	var buf []byte
	buf = make([]byte, 1024)
	for {
		n, err := reader.Read(buf)
		if err != nil && err != io.EOF {
			return "", errors.Wrap(err, "error reading file")
		}
		if n == 0 {
			break
		}
		if _, writeErr := file.Write(buf[:n]); writeErr != nil {
			return "", errors.Wrap(err, "error writing file")
		}
		if err == io.EOF {
			break
		}
	}

	if err = reader.Close(); err != nil {
		return "", errors.Wrap(err, "error reading downloaded file")
	}
	if err = file.Close(); err != nil {
		return "", errors.Wrap(err, "error closing file")
	}
	return fileName, nil
}

// SnapshotHistory retrieves snapshot history from S3.
func (s *S3) SnapshotHistory() (*SnapshotHistory, error) {
	prefix := fmt.Sprintf("%s/%s", s.config.AwsBasePath, s.config.Keyspace)
	params := &s3.ListObjectsV2Input{
		Bucket: aws.String(s.config.AwsBucket),
		Prefix: aws.String(prefix),
	}
	h := NewSnapshotHistory()
	for {
		resp, err := s.svc.ListObjectsV2(params)
		if err != nil {
			return nil, errors.Wrap(err, "error listing from S3")
		}
		for _, obj := range resp.Contents {
			h.Add(*obj.Key)
		}
		if !*resp.IsTruncated {
			break
		}
		params.ContinuationToken = resp.NextContinuationToken
	}
	return h, nil
}
