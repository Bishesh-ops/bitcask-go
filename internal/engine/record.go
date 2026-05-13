package engine

import(
	"encoding/binary"
	"hash/crc32"
	"time"
)

// Header size defines the fixed byte size of our record metadata
//CRC (4) + TImestamp (8) + KeySize (4) +ValueSize (4) = 20 bytes

const HeaderSize = 20

// Record represents the unpacked, in-memory format of our disk entry.
type Record struct{
	CRC uint32
	Timestamp uint64
	KeySize uint32
	ValueSize uint32
	Key []byte
	Value []byte
}

//Encode packs a key and value into a raw byte slice ready for disk I/O.

func Encode(key, value []byte) [] byte{
	keySz := uint32(len(key))
	valSz := uint32(len(value))
	timestamp := uint64(time.Now().UnixMicro())

	// Allocate exact buffer size: Header + Key Payload + Value Payload 
	// This is like malloc in C
	buf := make([]byte, HeaderSize+keySz+valSz)
	// Pack the metadata fields with using explicit Endianness.
	// We skip the first 4 bytes [0:4] because we'll drop CRC checksum at the end.
	binary.LittleEndian.PutUint64(buf[4:12], timestamp)
	binary.LittleEndian.PutUint32(buf[12:16], keySz)
	binary.LittleEndian.PutUint32(buf[16:20], valSz)
	// Copy the variable payloads directly into the allocated buffer space
	copy(buf[20:20+keySz], key)
	copy(buf[20+keySz:], value)
	//Compute the CRC checksum over everything excet the 4 bytes allocated for CRC themselves
	crc := crc32.ChecksumIEEE(buf[4:])
	binary.LittleEndian.AppendUint32(buf[0:4], crc)

	return buf
}


