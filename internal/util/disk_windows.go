//go:build windows

package util

import (
	"syscall"
	"unsafe"
)

// DiskUsagePct returns the disk usage percentage for the filesystem containing the given path.
// Returns 0 on error.
func DiskUsagePct(path string) int {
	kernel32 := syscall.NewLazyDLL("kernel32.dll")
	getDiskFreeSpaceEx := kernel32.NewProc("GetDiskFreeSpaceExW")

	var freeBytesAvailable, totalBytes, totalFreeBytes uint64
	pathPtr, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return 0
	}

	ret, _, _ := getDiskFreeSpaceEx.Call(
		uintptr(unsafe.Pointer(pathPtr)),
		uintptr(unsafe.Pointer(&freeBytesAvailable)),
		uintptr(unsafe.Pointer(&totalBytes)),
		uintptr(unsafe.Pointer(&totalFreeBytes)),
	)
	if ret == 0 {
		return 0
	}
	if totalBytes == 0 {
		return 0
	}
	used := totalBytes - totalFreeBytes
	return int(used * 100 / totalBytes)
}
