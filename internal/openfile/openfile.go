package openfile

import "os"

type Options struct {
	FailIfFileExists      bool
	FailIfFileDoesntExist bool
}

func OpenFile(opts Options) func(string, int, os.FileMode) (*os.File, error) {
	if opts.FailIfFileExists {
		return func(pathname string, flags int, mode os.FileMode) (*os.File, error) {
			return os.OpenFile(pathname, flags|os.O_EXCL, mode)
		}
	}

	if opts.FailIfFileDoesntExist {
		return func(pathname string, flags int, mode os.FileMode) (*os.File, error) {
			return os.OpenFile(pathname, flags&^os.O_CREATE, mode)
		}
	}

	return os.OpenFile
}
