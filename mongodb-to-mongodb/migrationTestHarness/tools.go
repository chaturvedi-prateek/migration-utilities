package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// tools holds resolved absolute paths to every external binary the harness drives.
type tools struct {
	mongod       string
	mongosh      string
	mongosync    string
	orchestrator string
}

// Versions to download (x86_64 Linux). The distro token is detected at runtime so
// the server/mongosync builds match the host's system libraries.
const (
	serverVersion    = "7.0.22" // must be >= 7.0.18 for mongosync 1.21 compatibility
	mongosyncVersion = "1.21.0"
	mongoshVersion   = "2.3.8" // mongosh linux-x64 build is distro-agnostic
)

// detectDistro returns the MongoDB download distro token (e.g. "amazon2023",
// "ubuntu2204", "rhel90") for the current host, read from /etc/os-release.
func detectDistro() string {
	id, ver := "", ""
	if b, err := os.ReadFile("/etc/os-release"); err == nil {
		for _, line := range strings.Split(string(b), "\n") {
			switch {
			case strings.HasPrefix(line, "ID="):
				id = strings.Trim(strings.TrimPrefix(line, "ID="), `"`)
			case strings.HasPrefix(line, "VERSION_ID="):
				ver = strings.Trim(strings.TrimPrefix(line, "VERSION_ID="), `"`)
			}
		}
	}
	major := ver
	if i := strings.Index(ver, "."); i >= 0 {
		major = ver[:i]
	}
	switch id {
	case "amzn":
		if major == "2" {
			return "amazon2"
		}
		return "amazon2023"
	case "ubuntu":
		if strings.HasPrefix(ver, "20") {
			return "ubuntu2004"
		}
		return "ubuntu2204"
	case "debian":
		return "debian12"
	case "rhel", "centos", "rocky", "almalinux":
		if major == "8" {
			return "rhel80"
		}
		return "rhel90"
	default:
		return "amazon2023" // sensible EC2 default
	}
}

func defaultServerTgz() string {
	return fmt.Sprintf("https://fastdl.mongodb.org/linux/mongodb-linux-x86_64-%s-%s.tgz", detectDistro(), serverVersion)
}
func defaultMongosyncTgz() string {
	return fmt.Sprintf("https://fastdl.mongodb.org/tools/mongosync/mongosync-%s-x86_64-%s.tgz", detectDistro(), mongosyncVersion)
}
func defaultMongoshTgz() string {
	return fmt.Sprintf("https://downloads.mongodb.com/compass/mongosh-%s-linux-x64.tgz", mongoshVersion)
}

// resolveTools finds the MongoDB binaries on PATH or in toolDir, downloading the
// tarballs into toolDir when missing (unless skipDownload). Driver binaries
// (orchestrator / scaffold) are resolved lazily by the chosen driver, so a
// scaffold-only run does not require the orchestrator to be present.
func resolveTools(toolDir string, skipDownload bool) (*tools, error) {
	if err := os.MkdirAll(toolDir, 0o755); err != nil {
		return nil, err
	}
	t := &tools{}
	var err error

	logf("detected distro token: %s", detectDistro())
	if t.mongod, err = ensureBinary("mongod", toolDir, envOr("HARNESS_SERVER_TGZ", defaultServerTgz()), "bin/mongod", skipDownload); err != nil {
		return nil, err
	}
	if t.mongosh, err = ensureBinary("mongosh", toolDir, envOr("HARNESS_MONGOSH_TGZ", defaultMongoshTgz()), "bin/mongosh", skipDownload); err != nil {
		return nil, err
	}
	if t.mongosync, err = ensureBinary("mongosync", toolDir, envOr("HARNESS_MONGOSYNC_TGZ", defaultMongosyncTgz()), "bin/mongosync", skipDownload); err != nil {
		return nil, err
	}
	return t, nil
}

func locateOrchestrator(toolDir string) (string, error) {
	if p, err := exec.LookPath("mongosyncOrchestrator"); err == nil {
		return p, nil
	}
	candidates := []string{filepath.Join(toolDir, "mongosyncOrchestrator")}
	if self, err := os.Executable(); err == nil {
		candidates = append(candidates, filepath.Join(filepath.Dir(self), "mongosyncOrchestrator"))
	}
	for _, c := range candidates {
		if isExec(c) {
			return c, nil
		}
	}
	return "", fmt.Errorf("mongosyncOrchestrator not found on PATH, in %s, or next to the harness binary", toolDir)
}

// ensureBinary returns the path to `name`, preferring PATH, then a prior extraction
// in toolDir, otherwise downloading+extracting tgz and pulling out innerPath.
func ensureBinary(name, toolDir, tgzURL, innerPath string, skipDownload bool) (string, error) {
	if p, err := exec.LookPath(name); err == nil {
		return p, nil
	}
	dst := filepath.Join(toolDir, name)
	if isExec(dst) {
		return dst, nil
	}
	if skipDownload {
		return "", fmt.Errorf("%s not found and --skip-download set (looked on PATH and in %s)", name, toolDir)
	}
	logf("downloading %s from %s", name, tgzURL)
	if err := downloadAndExtract(tgzURL, innerPath, dst); err != nil {
		return "", fmt.Errorf("acquire %s: %w", name, err)
	}
	return dst, nil
}

// downloadAndExtract streams a .tgz and extracts the single member whose path ends
// with innerPath to dst (mode 0755). Uses the system tar to avoid pulling in deps.
func downloadAndExtract(url, innerPath, dst string) error {
	tmp, err := os.CreateTemp("", "harness-*.tgz")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	tmp.Close()

	if err := run("", "curl", "-fsSL", "-o", tmp.Name(), url); err != nil {
		return fmt.Errorf("download: %w", err)
	}
	// List archive members, find the one ending in innerPath (handles the version-
	// prefixed top-level dir), then extract just that file to stdout -> dst.
	out, err := output("", "tar", "-tzf", tmp.Name())
	if err != nil {
		return fmt.Errorf("list archive: %w", err)
	}
	var member string
	for _, line := range strings.Split(out, "\n") {
		if strings.HasSuffix(strings.TrimSpace(line), innerPath) {
			member = strings.TrimSpace(line)
			break
		}
	}
	if member == "" {
		return fmt.Errorf("%s not found inside archive", innerPath)
	}
	f, err := os.OpenFile(dst, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o755)
	if err != nil {
		return err
	}
	defer f.Close()
	cmd := exec.Command("tar", "-xzOf", tmp.Name(), member)
	cmd.Stdout = f
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func isExec(p string) bool {
	fi, err := os.Stat(p)
	return err == nil && !fi.IsDir() && fi.Mode()&0o111 != 0
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
