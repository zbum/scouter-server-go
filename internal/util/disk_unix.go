//go:build !windows

package util

import "syscall"

// DiskUsagePct returns the disk usage percentage for the filesystem containing the given path.
// Returns 0 on error.
func DiskUsagePct(path string) int {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		return 0
	}
	total := stat.Blocks * uint64(stat.Bsize)
	free := stat.Bavail * uint64(stat.Bsize)
	if total == 0 {
		return 0
	}
	used := total - free
	return int(used * 100 / total)
}
