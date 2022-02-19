package WAL

import (
	"hash/crc32"
	"path/filepath"
	"strings"
)

const (
	T_SIZE = 8
	C_SIZE = 4 //crc

	CRC_SIZE       = T_SIZE + C_SIZE
	TOMBSTONE_SIZE = CRC_SIZE + 1
	KEY_SIZE       = TOMBSTONE_SIZE + T_SIZE
	VALUE_SIZE     = KEY_SIZE + T_SIZE

	WAL_EXT     = "wal"
	END_EXT     = "_END"
	FORMAT_NAME = "00000000000000000000%d"
)

func CRC32(str string) uint32 {
	return crc32.ChecksumIEEE([]byte(str))
}

func fileNameWithoutExtension(fileName string) string {
	return strings.TrimSuffix(fileName, filepath.Ext(fileName))
}

func fileNameWithoutSuffix(fileName string, suffix string) string {
	return strings.TrimSuffix(fileName, suffix)
}

