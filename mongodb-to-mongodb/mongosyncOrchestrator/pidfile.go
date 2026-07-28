package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// pidFilePath is where a parallel/hold step records the PIDs of the mongosync
// processes it leaves running, so a later `commit`/`stop` can terminate them. The
// path is scoped per step so multiple held steps don't collide.
func pidFilePath(cfg *Config, step string) string {
	return filepath.Join(cfg.LogDir, "mongosync."+step+".pids")
}

// writePidFile records "id port pid" for each running instance of a step.
func writePidFile(cfg *Config, step string, instances []*instance) error {
	var b strings.Builder
	for _, in := range instances {
		if in.cmd != nil && in.cmd.Process != nil {
			fmt.Fprintf(&b, "%s %d %d\n", in.id, in.port, in.cmd.Process.Pid)
		}
	}
	return os.WriteFile(pidFilePath(cfg, step), []byte(b.String()), 0o644)
}

// stopByPidFile SIGTERMs every recorded PID (SIGKILL if it lingers), then removes
// the pid file. Missing file is not an error.
func stopByPidFile(cfg *Config, step string) error {
	path := pidFilePath(cfg, step)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	var firstErr error
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 3 {
			continue
		}
		id, pid := fields[0], mustAtoi(fields[2])
		if pid <= 0 {
			continue
		}
		if err := terminate(pid); err != nil {
			fmt.Printf("[%s] stop pid %d: %v\n", id, pid, err)
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		fmt.Printf("[%s] stopped (pid %d)\n", id, pid)
	}
	_ = os.Remove(path)
	return firstErr
}

// terminate sends SIGTERM, waits briefly for exit, then SIGKILLs if still alive.
func terminate(pid int) error {
	p, err := os.FindProcess(pid)
	if err != nil {
		return err
	}
	if err := p.Signal(syscall.SIGTERM); err != nil {
		if err.Error() == "os: process already finished" {
			return nil
		}
		return err
	}
	for i := 0; i < 20; i++ {
		if p.Signal(syscall.Signal(0)) != nil {
			return nil // gone
		}
		time.Sleep(500 * time.Millisecond)
	}
	_ = p.Signal(syscall.SIGKILL)
	return nil
}

func mustAtoi(s string) int {
	n, _ := strconv.Atoi(s)
	return n
}
