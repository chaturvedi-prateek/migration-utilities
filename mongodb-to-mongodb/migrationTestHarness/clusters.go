package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"
)

// node is one single-node replica set (a stand-in for one on-prem or one Atlas cluster).
type node struct {
	name    string // e.g. src01 / dst01
	rs      string // replica-set name
	port    int
	dbPath  string
	logPath string
	auth    bool // destinations run with auth+keyfile (mongosync needs a dest user)
	db      string // the single application database this cluster owns (disjoint)
	proc    *exec.Cmd
}

// clusterSet is the full topology: N sources + N destinations.
type clusterSet struct {
	sources []*node
	dests   []*node
	keyFile string
	t       *tools
}

const (
	srcBasePort = 30001
	dstBasePort = 31001
	migUser     = "mig"
	migPass     = "migpass"
)

// provision creates and starts N source + N destination single-node replica sets.
func provision(t *tools, dataDir string, n int) (*clusterSet, error) {
	cs := &clusterSet{t: t, keyFile: filepath.Join(dataDir, "keyfile")}
	if err := writeKeyfile(cs.keyFile); err != nil {
		return nil, err
	}
	for i := 0; i < n; i++ {
		cs.sources = append(cs.sources, &node{
			name: fmt.Sprintf("src%02d", i+1), rs: fmt.Sprintf("srcrs%02d", i+1),
			port: srcBasePort + i, db: fmt.Sprintf("appdb%02d", i+1),
			dbPath: filepath.Join(dataDir, fmt.Sprintf("src%02d", i+1)),
		})
		cs.dests = append(cs.dests, &node{
			name: fmt.Sprintf("dst%02d", i+1), rs: fmt.Sprintf("dstrs%02d", i+1),
			port: dstBasePort + i, db: fmt.Sprintf("appdb%02d", i+1), auth: true,
			dbPath: filepath.Join(dataDir, fmt.Sprintf("dst%02d", i+1)),
		})
	}
	for _, nd := range append(append([]*node{}, cs.sources...), cs.dests...) {
		if err := cs.startNode(nd); err != nil {
			return cs, err
		}
	}
	// Initiate replica sets, then create the migration user on each destination.
	for _, nd := range append(append([]*node{}, cs.sources...), cs.dests...) {
		if err := cs.initReplicaSet(nd); err != nil {
			return cs, err
		}
	}
	for _, nd := range cs.dests {
		if err := cs.createDestUser(nd); err != nil {
			return cs, err
		}
	}
	return cs, nil
}

func (cs *clusterSet) startNode(nd *node) error {
	if err := os.MkdirAll(nd.dbPath, 0o755); err != nil {
		return err
	}
	nd.logPath = nd.dbPath + ".log"
	args := []string{
		"--replSet", nd.rs, "--port", fmt.Sprint(nd.port),
		"--dbpath", nd.dbPath, "--bind_ip", "localhost",
		"--wiredTigerCacheSizeGB", "1", "--logpath", nd.logPath, "--fork",
	}
	if nd.auth {
		args = append(args, "--auth", "--keyFile", cs.keyFile)
	}
	logf("starting mongod %s (rs=%s port=%d auth=%v)", nd.name, nd.rs, nd.port, nd.auth)
	cmd := exec.Command(cs.t.mongod, args...)
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil { // --fork returns once the daemon is up
		return fmt.Errorf("start %s: %w", nd.name, err)
	}
	return nil
}

func (cs *clusterSet) initReplicaSet(nd *node) error {
	uri := fmt.Sprintf("mongodb://localhost:%d/", nd.port)
	js := fmt.Sprintf(`rs.initiate({_id:%q,members:[{_id:0,host:"localhost:%d"}]})`, nd.rs, nd.port)
	if _, err := mongoshEval(cs.t.mongosh, uri, js); err != nil {
		return fmt.Errorf("initiate %s: %w", nd.name, err)
	}
	// Wait for PRIMARY.
	return waitFor(nd.name+" primary", 60*time.Second, func() error {
		out, _ := mongoshEval(cs.t.mongosh, uri, `db.hello().isWritablePrimary`)
		if out == "true" {
			return nil
		}
		return fmt.Errorf("not primary yet")
	})
}

func (cs *clusterSet) createDestUser(nd *node) error {
	uri := fmt.Sprintf("mongodb://localhost:%d/", nd.port) // localhost exception
	js := fmt.Sprintf(`db.getSiblingDB("admin").createUser({user:%q,pwd:%q,roles:[{role:"root",db:"admin"}]})`, migUser, migPass)
	if _, err := mongoshEval(cs.t.mongosh, uri, js); err != nil {
		return fmt.Errorf("create user on %s: %w", nd.name, err)
	}
	logf("created migration user on %s", nd.name)
	return nil
}

// sourceURI / destURI produce the connection strings used in orchestrator.json.
func (nd *node) sourceURI() string {
	return fmt.Sprintf("mongodb://localhost:%d/?replicaSet=%s", nd.port, nd.rs)
}
func (nd *node) destURI() string {
	return fmt.Sprintf("mongodb://%s:%s@localhost:%d/?replicaSet=%s&authSource=admin",
		migUser, migPass, nd.port, nd.rs)
}

// teardown stops every mongod (best effort).
func (cs *clusterSet) teardown() {
	for _, nd := range append(append([]*node{}, cs.sources...), cs.dests...) {
		uri := fmt.Sprintf("mongodb://localhost:%d/", nd.port)
		if nd.auth {
			uri = nd.destURI()
		}
		_, _ = mongoshEval(cs.t.mongosh, uri, `db.getSiblingDB("admin").shutdownServer()`)
	}
}

func writeKeyfile(path string) error {
	out, err := output("", "openssl", "rand", "-base64", "756")
	if err != nil {
		return fmt.Errorf("generate keyfile: %w", err)
	}
	if err := os.WriteFile(path, []byte(out), 0o400); err != nil {
		return err
	}
	return nil
}
