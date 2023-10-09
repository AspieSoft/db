package main

import (
	"errors"
	"os"
	"path/filepath"
	"sync"

	"github.com/AspieSoft/goutil/v7"
	"github.com/alphadose/haxmap"
)

type CustomDatabase struct {
	File *os.File
	Path string
	BitSize uint16
	PrefixList []byte
	Cache *haxmap.Map[string, *Table]
	MU sync.Mutex
}

func NewCustom(path string, bitSize uint16, prefixList []byte) (*CustomDatabase, error) {
	builtinPrefixes := []byte("%=,@-!")
	for _, prefix := range prefixList {
		if goutil.Contains(builtinPrefixes, prefix) {
			return &CustomDatabase{}, errors.New("'"+string(prefix)+"' is a builtin prefix")
		}
	}

	path, err := filepath.Abs(path)
	if err != nil {
		return &CustomDatabase{}, err
	}

	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0755)
	if err != nil {
		return &CustomDatabase{}, err
	}

	if bitSize == 0 {
		bitSize = 1024
	}else if DebugMode && bitSize < 16 {
		bitSize = 16
	}else if !DebugMode && bitSize < 64 {
		bitSize = 64
	}else if bitSize > 64000 {
		bitSize = 64000
	}

	return &CustomDatabase{
		File: file,
		Path: path,
		BitSize: bitSize,
		PrefixList: []byte("$:"),
		Cache: haxmap.New[string, *Table](),
	}, nil
}
