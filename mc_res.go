package gomemcached

import (
	"encoding/binary"
	"fmt"
	"io"
	"unsafe"
)

// A memcached response
type MCResponse struct {
	magic uint8
	// The command opcode of the command that sent the request
	Opcode CommandCode
	klen   uint16
	elen   uint8
	dt     uint8
	// The status of the response
	Status Status
	blen   uint32
	// The opaque sent in the request
	Opaque uint32
	// The CAS identifier (if applicable)
	Cas uint64
}

func (res *MCResponse) data(start uintptr, end uintptr) []byte {
	return (*(*[1 << 30]byte)(unsafe.Pointer(res)))[start+HDR_LEN : end+HDR_LEN]
}

func (res *MCResponse) Extras() (e []byte) {
	e = res.data(0, uintptr(res.elen))
	return
}

func (res *MCResponse) Key() (k []byte) {
	k = res.data(uintptr(res.elen), uintptr(res.elen)+uintptr(res.klen))
	return
}

func (res *MCResponse) Body() (b []byte) {
	b = res.data(uintptr(res.elen)+uintptr(res.klen), uintptr(res.blen))
	return
}

// A debugging string representation of this response
func (res MCResponse) String() string {
	return fmt.Sprintf("{MCResponse status=%v keylen=%d, extralen=%d, bodylen=%d}",
		res.Status, res.klen, res.elen, res.blen-uint32(res.klen)-uint32(res.elen))
}

// Response as an error.
func (res *MCResponse) Error() string {
	return fmt.Sprintf("MCResponse status=%v, opcode=%v, opaque=%v, msg: %s",
		res.Status, res.Opcode, res.Opaque, string(res.Body()))
}

func errStatus(e error) Status {
	status := Status(0xffff)
	if res, ok := e.(*MCResponse); ok {
		status = res.Status
	}
	return status
}

// True if this error represents a "not found" response.
func IsNotFound(e error) bool {
	return errStatus(e) == KEY_ENOENT
}

// False if this error isn't believed to be fatal to a connection.
func IsFatal(e error) bool {
	if e == nil {
		return false
	}
	switch errStatus(e) {
	case KEY_ENOENT, KEY_EEXISTS, NOT_STORED, TMPFAIL:
		return false
	}
	return true
}

// Number of bytes this response consumes on the wire.
func (res *MCResponse) Size() int {
	return int(HDR_LEN) + int(res.blen)
}

func (hdr MCResponse) SetData(extras []byte, key []byte, body []byte) (res *MCResponse) {
	all := make([]byte, int(HDR_LEN)+len(extras)+len(key)+len(body))
	res = (*MCResponse)(unsafe.Pointer(&all[0]))

	*res = hdr
	res.elen = uint8(len(extras))
	res.klen = uint16(len(key))
	res.blen = uint32(len(body) + len(extras) + len(key))
	res.magic = RES_MAGIC
	res.dt = 0

	copy(all[int(HDR_LEN):], extras)
	copy(all[int(HDR_LEN)+len(extras):], key)
	copy(all[int(HDR_LEN)+len(extras)+len(key):], body)

	return
}

func (res *MCResponse) fixByteOrder() {
	binary.BigEndian.PutUint16((*(*[2]byte)(unsafe.Pointer(&res.klen)))[:], res.klen)
	binary.BigEndian.PutUint16((*(*[2]byte)(unsafe.Pointer(&res.Status)))[:], uint16(res.Status))
	binary.BigEndian.PutUint32((*(*[4]byte)(unsafe.Pointer(&res.blen)))[:], res.blen)
	binary.BigEndian.PutUint32((*(*[4]byte)(unsafe.Pointer(&res.Opaque)))[:], res.Opaque)
	binary.BigEndian.PutUint64((*(*[8]byte)(unsafe.Pointer(&res.Cas)))[:], res.Cas)
}

// Get just the header bytes for this response.
func (res *MCResponse) HeaderBytes() []byte {
	l := int(HDR_LEN) + int(res.elen) + int(res.klen)
	res.fixByteOrder()
	return ((*[1 << 30]byte)(unsafe.Pointer(res)))[0:l]
}

// The actual bytes transmitted for this response.
func (res *MCResponse) Bytes() []byte {
	l := int(HDR_LEN) + int(res.blen)
	res.fixByteOrder()
	return ((*[1 << 30]byte)(unsafe.Pointer(res)))[0:l]
}

// Send this response message across a writer.
func (res *MCResponse) Transmit(w io.Writer) (n int, err error) {
	if res.blen < 128 {
		n, err = w.Write(res.Bytes())
	} else {
		bts := res.Body()
		n, err = w.Write(res.HeaderBytes())
		if err == nil {
			m := 0
			m, err = w.Write(bts)
			m += n
		}
	}
	return
}

// Fill this MCResponse with the data from this reader.
func ReceiveResponse(r io.Reader, hdrBytes []byte) (res *MCResponse, n int, err error) {
	var m int
	var blen uint32
	if cap(hdrBytes) < int(HDR_LEN) {
		hdrBytes = make([]byte, HDR_LEN)
	} else {
		hdrBytes = hdrBytes[0:HDR_LEN]
	}

	n, err = io.ReadFull(r, hdrBytes)
	if err != nil {
		return nil, n, err
	}

	if hdrBytes[0] != RES_MAGIC {
		return nil, n, fmt.Errorf("Bad magic: 0x%02x", hdrBytes[0])
	}

	blen = binary.BigEndian.Uint32(hdrBytes[8:12])

	if int(blen) > MaxBodyLen {
		return nil, n, fmt.Errorf("%d is too big (max %d)", blen, MaxBodyLen)
	}

	bts := make([]byte, int(HDR_LEN)+int(blen))
	m, err = io.ReadFull(r, bts[HDR_LEN:])
	n += m

	res = (*MCResponse)(unsafe.Pointer(&bts[0]))
	res.magic = hdrBytes[0]
	res.Opcode = CommandCode(hdrBytes[1])
	res.klen = binary.BigEndian.Uint16(hdrBytes[2:4])
	res.elen = hdrBytes[4]
	res.dt = hdrBytes[5]
	res.Status = Status(binary.BigEndian.Uint16(hdrBytes[6:8]))
	res.blen = blen
	res.Opaque = binary.BigEndian.Uint32(hdrBytes[12:16])
	res.Cas = binary.BigEndian.Uint64(hdrBytes[16:24])

	return
}
