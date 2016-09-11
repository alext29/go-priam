package prium

import (
	"bytes"
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
	"sort"
	"strings"
)

// S3 ...
type S3 struct {
	config   *Config
	agent    *agent
	svc      *s3.S3
	uploader *s3manager.Uploader
}

// NewS3 ..
func NewS3(config *Config) *S3 {
	return &S3{config: config}
}

// Init ..
func (s *S3) Init() error {
	// create new session
	sess := session.New(&aws.Config{
		Region:      aws.String(*s.config.awsRegion),
		Credentials: credentials.NewStaticCredentials(*s.config.awsAccessKey, *s.config.awsSecretKey, ""),
	})
	s.svc = s3.New(sess)
	s.uploader = s3manager.NewUploader(sess)
	s.agent = newAgent(s.config)
	return nil
}

// UploadFiles ...
func (s *S3) UploadFiles(env, keyspace, parent, timestamp, host string, files []string) error {
	glog.Infof("uploading files to s3...")
	for _, file := range files {
		key := getFileKey(env, keyspace, parent, timestamp, host, file, *s.config.incremental)
		glog.Infof("upload key: %s", key)

		// read bytes from file@host
		b, err := s.agent.readFile(host, file)
		if err != nil {
			glog.Errorf("error reading file %s on host %s", file, host)
			return err
		}

		// gzip files before uploading
		reader, writer := io.Pipe()
		go func() {
			gw := gzip.NewWriter(writer)
			r := bytes.NewReader(b)
			io.Copy(gw, r)
			gw.Close()
			writer.Close()
		}()

		// details of file to upload
		params := &s3manager.UploadInput{
			Bucket: aws.String(*s.config.awsBucket),
			Body:   reader,
			Key:    aws.String(key),
		}

		// upload file
		_, err = s.uploader.Upload(params)
		if err != nil {
			glog.Errorf("error uploading file to S3 :: %v", err)
			//return errors.Wrap(err, "error uploading file to S3")
		}
	}
	return nil
}

func getFileKey(environment, keyspace, parent, timestamp, host, file string, incremental bool) string {
	dir, base := path.Split(path.Clean(file))
	dir, _ = path.Split(path.Clean(dir))
	if !incremental {
		dir, _ = path.Split(path.Clean(dir))
	}
	return fmt.Sprintf("/%s/%s/%s/%s/%s%s%s.gz",
		environment, keyspace, parent, timestamp, host, dir, base)
}

func (s *S3) downloadKeys(keys []string, prefix string) error {
	for _, key := range keys {
		glog.Infof("key: %s", key)
		err := s.downloadKey(key, prefix)
		if err != nil {
			return errors.Wrap(err, "error downloading key")
		}
	}
	return nil
}

func (s *S3) downloadKey(key, prefix string) error {
	glog.Infof("download key: %s", key)
	fileName := strings.TrimSuffix(fmt.Sprintf("%s/%s", prefix, key), ".gz")
	glog.Infof("to file: %s", fileName)
	params := &s3.GetObjectInput{
		Bucket: aws.String(*s.config.awsBucket),
		Key:    aws.String(key),
	}
	resp, err := s.svc.GetObject(params)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("error downloading key: %s", key))
	}

	dir := path.Dir(fileName)
	err = os.MkdirAll(dir, os.ModeDir|os.ModePerm)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("error creading dir %s", dir))
	}

	file, err := os.Create(fileName)
	if err != nil {
		return errors.Wrap(err, "error opening file")
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
			return errors.Wrap(err, "error reading file")
		}
		if n == 0 {
			break
		}
		if _, writeErr := file.Write(buf[:n]); writeErr != nil {
			return errors.Wrap(err, "error writing file")
		}
		if err == io.EOF {
			break
		}
	}

	if err = reader.Close(); err != nil {
		return errors.Wrap(err, "error reading downloaded file")
	}
	if err = file.Close(); err != nil {
		return errors.Wrap(err, "error closing file")
	}
	return nil
}

// GetSnapshotHistory retrieves snapshot history from S3.
func (s *S3) GetSnapshotHistory(env, keyspace string) (*SnapshotHistory, error) {
	prefix := fmt.Sprintf("%s/%s", env, keyspace)
	params := &s3.ListObjectsV2Input{
		Bucket: aws.String(*s.config.awsBucket),
		Prefix: aws.String(prefix),
	}
	resp, err := s.svc.ListObjectsV2(params)
	if err != nil {
		return nil, errors.Wrap(err, "error listing from S3")
	}
	h := NewSnapshotHistory()
	for _, obj := range resp.Contents {
		h.Add(*obj.Key)
	}
	return h, nil
}

// SnapshotHistory provides the history of all snapshots in S3 for a given environment and keyspace
type SnapshotHistory struct {
	parent map[string]string   // parent of a snapshot if incremental
	keys   map[string][]string // list of keys for given snapshot
}

// NewSnapshotHistory  ..
func NewSnapshotHistory() *SnapshotHistory {
	return &SnapshotHistory{
		parent: make(map[string]string),
		keys:   make(map[string][]string),
	}
}

// Add key to snapshot history
func (h *SnapshotHistory) Add(key string) {
	parts := strings.Split(key, "/")
	parent := parts[2]
	timestamp := parts[3]
	if parent != timestamp {
		h.parent[timestamp] = parent
	}
	h.keys[timestamp] = append(h.keys[timestamp], key)
}

// List returns a ordered list of timestamps.
func (h *SnapshotHistory) List() []string {
	var timestamps []string
	for timestamp := range h.keys {
		timestamps = append(timestamps, timestamp)
	}
	sort.Strings(timestamps)
	return timestamps
}

// Keys returns all keys for a given snapshot including keys for
// parent snapshots if this is an incremental backup
func (h *SnapshotHistory) Keys(snapshot string) ([]string, error) {
	var keys []string
	for {
		k, ok := h.keys[snapshot]
		if !ok {
			return nil, fmt.Errorf("did not find snapshot %s", snapshot)
		}
		keys = append(keys, k...)
		snapshot, ok = h.parent[snapshot]
		if !ok {
			break
		}
	}
	return keys, nil
}

// Valid returns true if this snapshot exists in history
func (h *SnapshotHistory) Valid(snapshot string) bool {
	_, ok := h.keys[snapshot]
	return ok
}
