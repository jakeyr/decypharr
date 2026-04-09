//go:build !linux

package vfs

import "os"

func dropFileCache(_ *os.File, _, _ int64) {}
