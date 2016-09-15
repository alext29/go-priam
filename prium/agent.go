package prium

import (
	"fmt"
	"github.com/golang/glog"
	"github.com/pkg/errors"
	"golang.org/x/crypto/ssh"
	"io"
	"io/ioutil"
	"os/exec"
	"path"
	"strings"
)

// Agent provides methods to run commands and interface with remote
// cassandra cluster nodes via ssh.
// TODO: add mutex to agent operations, to enable multi thread.
type Agent struct {
	user       string
	privateKey string
	clients    map[string]*ssh.Client
}

// NewAgent returns a new Agent.
func NewAgent(config *Config) *Agent {
	return &Agent{
		user:       config.User,
		privateKey: config.PrivateKey,
		clients:    make(map[string]*ssh.Client),
	}
}

// scpOpts are options provided for copying files to remote files via scp.
var scpOpts = []string{
	"-o", "PasswordAuthentication=no",
	"-o", "CheckHostIP=no",
	"-o", "ChallengeResponseAuthentication=no",
	"-o", "StrictHostKeyChecking=no",
	"-o", "KbdInteractiveAuthentication=no",
	"-o", "BatchMode=yes",
}

// UploadFile from local machine to remote host.
func (a *Agent) UploadFile(host, localFile, remotePath string) error {

	// create remote dir
	_, err := a.Run(host, fmt.Sprintf("mkdir -p %s", remotePath))
	if err != nil {
		return errors.Wrap(err, "could not create remote directory")
	}

	// copy file
	cmd := exec.Command("scp")
	cmd.Args = append(cmd.Args, scpOpts...)
	cmd.Args = append(cmd.Args, localFile)
	dst := fmt.Sprintf("%s@%s:%s", a.user, host, remotePath)
	cmd.Args = append(cmd.Args, dst)
	if _, err := cmd.CombinedOutput(); err != nil {
		return errors.Wrap(err, "could not copy file to cassandra host")
	}
	return nil
}

// ListDirs on remote host in given directory.
func (a *Agent) ListDirs(host, dir string) ([]string, error) {
	return a.List(host, dir, "d")
}

// ListFiles on remote host in given directory.
func (a *Agent) ListFiles(host, dir string) ([]string, error) {
	return a.List(host, dir, "f")
}

// List files of given type in directory on remote host. Does not run recursive.
func (a *Agent) List(host, dir, t string) ([]string, error) {
	dir = path.Clean(dir)
	bytes, err := a.Run(host, fmt.Sprintf("find %s -maxdepth 1 -type %s", dir, t))
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("error listing dir %s on host %s", dir, host))
	}
	return strings.Split(string(bytes), "\n"), nil
}

// ReadFile from remote machine and return bytes.
func (a *Agent) ReadFile(host, file string) (io.Reader, error) {

	s, err := a.session(host)
	if err != nil {
		return nil, errors.Wrap(err, "error getting ssh session")
	}
	cmd := fmt.Sprintf("cat %s", file)
	out, err := s.StdoutPipe()
	if err != nil {
		return nil, errors.Wrap(err, "error getting stdout pipe")
	}
	err = s.Start(cmd)
	if err != nil {
		return nil, errors.Wrap(err, "error reading file")
	}
	return out, nil
}

// Run command on remote host and return combined stderr and stdout outputs.
func (a *Agent) Run(host, cmd string) ([]byte, error) {
	s, err := a.session(host)
	if err != nil {
		return nil, errors.Wrap(err, "ssh session failed")
	}
	glog.V(2).Infof("run@%s: %s", host, cmd)
	return s.CombinedOutput(cmd)
}

// session creates a new ssh session to host.
func (a *Agent) session(host string) (*ssh.Session, error) {

	client, err := a.client(host)
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("host: %s", host))
	}

	session, err := client.NewSession()
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("host: %s", host))
	}
	return session, nil
}

// client creates ssh client to host if one does not already exists.
func (a *Agent) client(host string) (*ssh.Client, error) {

	if host == "" {
		return nil, fmt.Errorf("empty cassandra host")
	}

	if session, ok := a.clients[host]; ok {
		return session, nil
	}

	key, err := ioutil.ReadFile(a.privateKey)
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("error reading private key %s", a.privateKey))
	}

	signer, err := ssh.ParsePrivateKey(key)
	if err != nil {
		return nil, errors.Wrap(err, "error parsing private key")
	}

	// ssh client config
	clientConfig := &ssh.ClientConfig{
		User: a.user,
		Auth: []ssh.AuthMethod{
			ssh.PublicKeys(signer),
		},
	}

	client, err := ssh.Dial("tcp", fmt.Sprintf("%s:22", host), clientConfig)
	if err != nil {
		return nil, errors.Wrap(err, "error connecting to host")
	}
	a.clients[host] = client
	return client, nil
}
