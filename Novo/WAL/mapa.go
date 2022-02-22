package WAL

import (
	"errors"
	mmap "github.com/edsrzf/mmap-go"
	"os"
)

func append_entry(file *os.File, data []byte) error {
	currentLen, err := fileLen(file)
	if err != nil {
		return err
	}
	err = file.Truncate(currentLen + int64(len(data)))
	if err != nil {
		return err
	}
	//mmapf, err := mmap.MapRegion(file, int(currentLen)+len(data), mmap.RDWR, 0, 0)
	mmapf, err := mmap.Map(file, mmap.RDWR, 0)
	if err != nil {
		return err
	}
	defer func(mmapf *mmap.MMap) {
		err := mmapf.Unmap()
		if err != nil {

		}
	}(&mmapf)
	copy(mmapf[currentLen:], data)
	err = mmapf.Flush()
	if err != nil {
		return err
	}
	return nil
}

// Map maps an entire file into memory

// prot argument
// mmap.RDONLY - Maps the memory read-only. Attempts to write to the MMap object will result in undefined behavior.
// mmap.RDWR - Maps the memory as read-write. Writes to the MMap object will update the underlying file.
// mmap.COPY - Writes to the MMap object will affect memory, but the underlying file will remain unchanged.
// mmap.EXEC - The mapped memory is marked as executable.

// flag argument
// mmap.ANON - The mapped memory will not be backed by a file. If ANON is set in flags, f is ignored.
func read(file *os.File) ([]byte, error) {
	mmapf, err := mmap.Map(file, mmap.RDONLY, 0)
	if err != nil {
		return nil, err
	}
	defer func(mmapf *mmap.MMap) {
		err := mmapf.Unmap()
		if err != nil {

		}
	}(&mmapf)
	result := make([]byte, len(mmapf))
	copy(result, mmapf)
	return result, nil
}

func readRange(file *os.File, startIndex, endIndex int) ([]byte, error) {
	if startIndex < 0 || endIndex < 0 || startIndex > endIndex {
		return nil, errors.New("indices invalid")
	}
	mmapf, err := mmap.Map(file, mmap.RDONLY, 0)
	if err != nil {
		return nil, err
	}
	defer func(mmapf *mmap.MMap) {
		err := mmapf.Unmap()
		if err != nil {

		}
	}(&mmapf)
	if startIndex >= len(mmapf) || endIndex > len(mmapf) {
		return nil, errors.New("indices invalid")
	}
	result := make([]byte, endIndex-startIndex)
	copy(result, mmapf[startIndex:endIndex])
	return result, nil
}

func fileLen(file *os.File) (int64, error) {
	info, err := file.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}
