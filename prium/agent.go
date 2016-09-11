package prium

import (
	"fmt"
	"github.com/golang/glog"
	"github.com/pkg/errors"
	"golang.org/x/crypto/ssh"
	"io/ioutil"
	"strings"
)

// agent ...
type agent struct {
	user       string
	privateKey string
	clients    map[string]*ssh.Client
}

// newAgent ...
func newAgent(config *Config) *agent {
	return &agent{
		user:       *config.user,
		privateKey: *config.privateKey,
		clients:    make(map[string]*ssh.Client),
	}
}

// list all directories in given directory
func (a *agent) listDirs(host, dir string) ([]string, error) {
	return a.list(host, dir, "d")
}

// list all files in directory
func (a *agent) listFiles(host, dir string) ([]string, error) {
	return a.list(host, dir, "f")
}

// list files of given type in directory
func (a *agent) list(host, dir, t string) ([]string, error) {
	bytes, err := a.run(host, fmt.Sprintf("find %s -maxdepth 1 -type %s", dir, t))
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("error listing dir %s on host %s", dir, host))
	}
	return strings.Split(string(bytes), "\n"), nil
}

func (a *agent) readFile(host, file string) ([]byte, error) {
	return a.run(host, fmt.Sprintf("cat %s", file))
}

// run runs command on host and returns combined stderr and stdout output.
func (a *agent) run(host, cmd string) ([]byte, error) {
	s, err := a.session(host)
	if err != nil {
		glog.Errorf("error establishing session to host %s :: %v", host, err)
		return nil, err
	}
	return s.CombinedOutput(cmd)
}

// session creates a new ssh session to host.
func (a *agent) session(host string) (*ssh.Session, error) {

	client, err := a.client(host)
	if err != nil {
		glog.Error("did not get ssh client")
		return nil, fmt.Errorf("did not get ssh client")
	}

	session, err := client.NewSession()
	if err != nil {
		fmt.Printf("error creating new session :: %v", err)
		return nil, err
	}
	return session, nil
}

// client creates ssh client to host if one does not already exists.
func (a *agent) client(host string) (*ssh.Client, error) {

	if host == "" {
		glog.Error("empty cassandra host")
		return nil, fmt.Errorf("empty cassandra host")
	}

	if session, ok := a.clients[host]; ok {
		return session, nil
	}

	glog.V(2).Infof("using private key: %s", a.privateKey)
	glog.V(2).Infof("user: %s", a.user)
	key, err := ioutil.ReadFile(a.privateKey)
	if err != nil {
		glog.Errorf("error reading private key %s :: %v", a.privateKey, err)
		return nil, err
	}

	signer, err := ssh.ParsePrivateKey(key)
	if err != nil {
		glog.Errorf("error parsing private key :: %v", err)
		return nil, err
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
		glog.Errorf("error connecting to host %s :: %v", host, err)
		return nil, err
	}

	a.clients[host] = client
	return client, nil
}
