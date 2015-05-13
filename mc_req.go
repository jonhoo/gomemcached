package gomemcached

import (
	"encoding/binary"
	"fmt"
	"io"
	"unsafe"
)

// The maximum reasonable body length to expect.
// Anything larger than this will result in an error.
var MaxBodyLen = int(1e6)

// A Memcached Request
type MCRequest struct {
	magic uint8
	// The command being issued
	Opcode CommandCode
	klen   uint16
	elen   uint8
	dt     uint8
	// The vbucket to which this command belongs
	VBucket uint16
	blen    uint32
	// An opaque value to be returned with this request
	Opaque uint32
	// The CAS (if applicable, or 0)
	Cas uint64
	// Command extras, key, and body follow
}

func (req *MCRequest) data(start uintptr, end uintptr) []byte {
	return (*(*[1 << 30]byte)(unsafe.Pointer(req)))[start+HDR_LEN : end+HDR_LEN]
}

func (req *MCRequest) real_elen() uintptr {
	if req.Opcode >= TAP_MUTATION &&
		req.Opcode <= TAP_CHECKPOINT_END &&
		req.blen != 0 {
		// In these commands there is "engine private"
		// data at the end of the extras.  The first 2
		// bytes of extra data give its length.
		return uintptr(req.elen) + uintptr(binary.BigEndian.Uint16(req.data(0, 2)))
	}
	return uintptr(req.elen)
}

func (req *MCRequest) Extras() (e []byte) {
	e = req.data(0, req.real_elen())
	return
}

func (req *MCRequest) Key() (k []byte) {
	elen := req.real_elen()
	k = req.data(elen, elen+uintptr(req.klen))
	return
}

func (req *MCRequest) Body() (b []byte) {
	b = req.data(req.real_elen()+uintptr(req.klen), uintptr(req.blen))
	return
}

// The number of bytes this request requires.
func (req MCRequest) Size() int {
	return int(HDR_LEN) + int(req.blen)
}

// A debugging string representation of this request
func (req *MCRequest) String() string {
	return fmt.Sprintf("{MCRequest opcode=%s, bodylen=%d, key='%s'}",
		req.Opcode, int(req.blen)-int(req.klen)-int(req.real_elen()), req.Key())
}

func (hdr MCRequest) SetData(extras []byte, key []byte, body []byte) (req *MCRequest) {
	all := make([]byte, int(HDR_LEN)+len(extras)+len(key)+len(body))
	req = (*MCRequest)(unsafe.Pointer(&all[0]))

	*req = hdr
	req.elen = uint8(len(extras))
	req.klen = uint16(len(key))
	req.blen = uint32(len(body) + len(extras) + len(key))
	req.magic = REQ_MAGIC
	req.dt = 0

	copy(all[int(HDR_LEN):], extras)
	copy(all[int(HDR_LEN)+len(extras):], key)
	copy(all[int(HDR_LEN)+len(extras)+len(key):], body)

	return
}

func (req *MCRequest) fixByteOrder() {
	binary.BigEndian.PutUint16((*(*[2]byte)(unsafe.Pointer(&req.klen)))[:], req.klen)
	binary.BigEndian.PutUint16((*(*[2]byte)(unsafe.Pointer(&req.VBucket)))[:], req.VBucket)
	binary.BigEndian.PutUint32((*(*[4]byte)(unsafe.Pointer(&req.blen)))[:], req.blen)
	binary.BigEndian.PutUint32((*(*[4]byte)(unsafe.Pointer(&req.Opaque)))[:], req.Opaque)
	binary.BigEndian.PutUint64((*(*[8]byte)(unsafe.Pointer(&req.Cas)))[:], req.Cas)
}

// The wire representation of the header (with the extras and key)
func (req *MCRequest) HeaderBytes() []byte {
	l := int(HDR_LEN) + int(req.real_elen()) + int(req.klen)
	req.fixByteOrder()
	return ((*[1 << 30]byte)(unsafe.Pointer(req)))[0:l]
}

// The wire representation of this request.
func (req *MCRequest) Bytes() []byte {
	l := int(HDR_LEN) + int(req.blen)
	req.fixByteOrder()
	return ((*[1 << 30]byte)(unsafe.Pointer(req)))[0:l]
}

// Send this request message across a writer.
func (req *MCRequest) Transmit(w io.Writer) (n int, err error) {
	if req.blen < 128 {
		n, err = w.Write(req.Bytes())
	} else {
		bts := req.Body()
		n, err = w.Write(req.HeaderBytes())
		if err == nil {
			m := 0
			m, err = w.Write(bts)
			n += m
		}
	}
	return
}

func ReceiveRequest(r io.Reader, hdrBytes []byte) (req *MCRequest, n int, err error) {
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

	if hdrBytes[0] != REQ_MAGIC {
		return nil, n, fmt.Errorf("Bad magic: 0x%02x", hdrBytes[0])
	}

	blen = binary.BigEndian.Uint32(hdrBytes[8:12])

	if int(blen) > MaxBodyLen {
		return nil, n, fmt.Errorf("%d is too big (max %d)", blen, MaxBodyLen)
	}

	bts := make([]byte, int(HDR_LEN)+int(blen))
	m, err = io.ReadFull(r, bts[HDR_LEN:])
	n += m

	req = (*MCRequest)(unsafe.Pointer(&bts[0]))
	req.magic = hdrBytes[0]
	req.Opcode = CommandCode(hdrBytes[1])
	req.klen = binary.BigEndian.Uint16(hdrBytes[2:4])
	req.elen = hdrBytes[4]
	req.dt = hdrBytes[5]
	req.VBucket = binary.BigEndian.Uint16(hdrBytes[6:8])
	req.blen = blen
	req.Opaque = binary.BigEndian.Uint32(hdrBytes[12:16])
	req.Cas = binary.BigEndian.Uint64(hdrBytes[16:24])

	return
}

// these
// lines
// are
// needed
// for
// pprof
// https://codereview.appspot.com/172600043
