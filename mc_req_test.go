package gomemcached

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"reflect"
	"testing"
)

func TestEncodingRequest(t *testing.T) {
	req := MCRequest{
		Opcode:  SET,
		Cas:     938424885,
		Opaque:  7242,
		VBucket: 824,
	}.SetData(nil, []byte("somekey"), []byte("somevalue"))

	str := req.String()
	got := req.Bytes()

	expected := []byte{
		REQ_MAGIC, byte(SET),
		0x0, 0x7, // length of key
		0x0,       // extra length
		0x0,       // reserved
		0x3, 0x38, // vbucket
		0x0, 0x0, 0x0, 0x10, // Length of value
		0x0, 0x0, 0x1c, 0x4a, // opaque
		0x0, 0x0, 0x0, 0x0, 0x37, 0xef, 0x3a, 0x35, // CAS
		's', 'o', 'm', 'e', 'k', 'e', 'y',
		's', 'o', 'm', 'e', 'v', 'a', 'l', 'u', 'e'}

	if len(got) != len(expected) {
		t.Fatalf("Expected %v bytes, got %v", len(expected),
			len(got))
	}

	if !reflect.DeepEqual(got, expected) {
		t.Fatalf("Expected:\n%#v\n  -- got -- \n%#v",
			expected, got)
	}

	exp := `{MCRequest opcode=SET, bodylen=9, key='somekey'}`
	if str != exp {
		t.Errorf("Expected string=%q, got %q", exp, str)
	}
}

func TestEncodingRequestWithExtras(t *testing.T) {
	req := MCRequest{
		Opcode:  SET,
		Cas:     938424885,
		Opaque:  7242,
		VBucket: 824,
	}.SetData([]byte{1, 2, 3, 4}, []byte("somekey"), []byte("somevalue"))

	buf := &bytes.Buffer{}
	req.Transmit(buf)
	got := buf.Bytes()

	expected := []byte{
		REQ_MAGIC, byte(SET),
		0x0, 0x7, // length of key
		0x4,       // extra length
		0x0,       // reserved
		0x3, 0x38, // vbucket
		0x0, 0x0, 0x0, 0x14, // Length of remainder
		0x0, 0x0, 0x1c, 0x4a, // opaque
		0x0, 0x0, 0x0, 0x0, 0x37, 0xef, 0x3a, 0x35, // CAS
		1, 2, 3, 4, // extras
		's', 'o', 'm', 'e', 'k', 'e', 'y',
		's', 'o', 'm', 'e', 'v', 'a', 'l', 'u', 'e'}

	if len(got) != len(expected) {
		t.Fatalf("Expected %v bytes, got %v", len(expected), len(got))
	}

	if !reflect.DeepEqual(got, expected) {
		t.Fatalf("Expected:\n%#v\n  -- got -- \n%#v",
			expected, got)
	}
}

func TestEncodingRequestWithLargeBody(t *testing.T) {
	req := MCRequest{
		Opcode:  SET,
		Cas:     938424885,
		Opaque:  7242,
		VBucket: 824,
	}.SetData([]byte{1, 2, 3, 4}, []byte("somekey"), make([]byte, 256))

	buf := &bytes.Buffer{}
	req.Transmit(buf)
	got := buf.Bytes()

	expected := append([]byte{
		REQ_MAGIC, byte(SET),
		0x0, 0x7, // length of key
		0x4,       // extra length
		0x0,       // reserved
		0x3, 0x38, // vbucket
		0x0, 0x0, 0x1, 0xb, // Length of remainder
		0x0, 0x0, 0x1c, 0x4a, // opaque
		0x0, 0x0, 0x0, 0x0, 0x37, 0xef, 0x3a, 0x35, // CAS
		1, 2, 3, 4, // extras
		's', 'o', 'm', 'e', 'k', 'e', 'y',
	}, make([]byte, 256)...)

	if len(got) != len(expected) {
		t.Fatalf("Expected %v bytes, got %v", len(expected), len(got))
	}

	if !reflect.DeepEqual(got, expected) {
		t.Fatalf("Expected:\n%#v\n  -- got -- \n%#v",
			expected, got)
	}
}

func BenchmarkEncodingRequest(b *testing.B) {
	req := MCRequest{
		Opcode:  SET,
		Cas:     938424885,
		Opaque:  7242,
		VBucket: 824,
	}.SetData(nil, []byte("somekey"), []byte("somevalue"))

	b.SetBytes(int64(req.Size()))

	for i := 0; i < b.N; i++ {
		req.Bytes()
	}
}

func BenchmarkEncodingRequest0CAS(b *testing.B) {
	req := MCRequest{
		Opcode:  SET,
		Cas:     0,
		Opaque:  7242,
		VBucket: 824,
	}.SetData(nil, []byte("somekey"), []byte("somevalue"))

	b.SetBytes(int64(req.Size()))

	for i := 0; i < b.N; i++ {
		req.Bytes()
	}
}

func BenchmarkEncodingRequest1Extra(b *testing.B) {
	req := MCRequest{
		Opcode:  SET,
		Cas:     0,
		Opaque:  7242,
		VBucket: 824,
	}.SetData([]byte{1}, []byte("somekey"), []byte("somevalue"))

	b.SetBytes(int64(req.Size()))

	for i := 0; i < b.N; i++ {
		req.Bytes()
	}
}

func TestRequestTransmit(t *testing.T) {
	res := MCRequest{}.SetData(nil, []byte("thekey"), nil)
	_, err := res.Transmit(ioutil.Discard)
	if err != nil {
		t.Errorf("Error sending small request: %v", err)
	}

	res = MCRequest{}.SetData(nil, []byte("thekey"), make([]byte, 256))
	_, err = res.Transmit(ioutil.Discard)
	if err != nil {
		t.Errorf("Error sending large request thing: %v", err)
	}

}

func TestReceiveRequest(t *testing.T) {
	req := MCRequest{
		Opcode:  SET,
		Cas:     0,
		Opaque:  7242,
		VBucket: 824,
	}.SetData([]byte{1}, []byte("somekey"), []byte("somevalue"))

	data := req.Bytes()

	// .Bytes() modifies req!
	req = MCRequest{
		Opcode:  SET,
		Cas:     0,
		Opaque:  7242,
		VBucket: 824,
	}.SetData([]byte{1}, []byte("somekey"), []byte("somevalue"))

	req2, n, err := ReceiveRequest(bytes.NewReader(data), nil)
	if err != nil {
		t.Fatalf("Error receiving: %v", err)
	}
	if len(data) != n {
		t.Errorf("Expected to read %v bytes, read %v", len(data), n)
	}

	if !reflect.DeepEqual(req, req2) {
		t.Fatalf("Expected %#v == %#v", req, req2)
	}
}

func TestReceiveRequestNoContent(t *testing.T) {
	req := MCRequest{
		Opcode:  SET,
		Cas:     0,
		Opaque:  7242,
		VBucket: 824,
	}.SetData(nil, nil, nil)

	data := req.Bytes()

	req = MCRequest{
		Opcode:  SET,
		Cas:     0,
		Opaque:  7242,
		VBucket: 824,
	}.SetData(nil, nil, nil)

	req2, n, err := ReceiveRequest(bytes.NewReader(data), nil)
	if err != nil {
		t.Fatalf("Error receiving: %v", err)
	}
	if len(data) != n {
		t.Errorf("Expected to read %v bytes, read %v", len(data), n)
	}

	if fmt.Sprintf("%#v", req) != fmt.Sprintf("%#v", req2) {
		t.Fatalf("Expected %#v == %#v", req, req2)
	}
}

func TestReceiveRequestShortHdr(t *testing.T) {
	req := MCRequest{}.SetData(nil, nil, nil)
	req, n, err := ReceiveRequest(bytes.NewReader([]byte{1, 2, 3}), nil)
	if err == nil {
		t.Errorf("Expected error, got %#v", req)
	}
	if n != 3 {
		t.Errorf("Expected to have read 3 bytes, read %v", n)
	}
}

func TestReceiveRequestShortBody(t *testing.T) {
	req := MCRequest{
		Opcode:  SET,
		Cas:     0,
		Opaque:  7242,
		VBucket: 824,
	}.SetData([]byte{1}, []byte("somekey"), []byte("somevalue"))

	data := req.Bytes()

	req2, n, err := ReceiveRequest(bytes.NewReader(data[:len(data)-3]), nil)
	if err == nil {
		t.Errorf("Expected error, got %#v", req2)
	}
	if n != len(data)-3 {
		t.Errorf("Expected to have read %v bytes, read %v", len(data)-3, n)
	}
}

func TestReceiveRequestBadMagic(t *testing.T) {
	req := MCRequest{
		Opcode:  SET,
		Cas:     0,
		Opaque:  7242,
		VBucket: 824,
	}.SetData([]byte{1}, []byte("somekey"), []byte("somevalue"))

	data := req.Bytes()
	data[0] = 0x83

	req2, _, err := ReceiveRequest(bytes.NewReader(data), nil)
	if err == nil {
		t.Fatalf("Expected error, got %#v", req2)
	}
}

func TestReceiveRequestLongBody(t *testing.T) {
	req := MCRequest{
		Opcode:  SET,
		Cas:     0,
		Opaque:  7242,
		VBucket: 824,
	}.SetData([]byte{1}, []byte("somekey"), make([]byte, MaxBodyLen+5))

	data := req.Bytes()

	req2, _, err := ReceiveRequest(bytes.NewReader(data), nil)
	if err == nil {
		t.Fatalf("Expected error, got %#v", req2)
	}
}

func BenchmarkReceiveRequest(b *testing.B) {
	req := MCRequest{
		Opcode:  SET,
		Cas:     0,
		Opaque:  7242,
		VBucket: 824,
	}.SetData([]byte{1}, []byte("somekey"), []byte("somevalue"))

	data := req.Bytes()
	data[0] = REQ_MAGIC
	rdr := bytes.NewReader(data)

	b.SetBytes(int64(len(data)))

	b.ResetTimer()
	buf := make([]byte, HDR_LEN)
	for i := 0; i < b.N; i++ {
		rdr.Seek(0, 0)
		_, _, err := ReceiveRequest(rdr, buf)
		if err != nil {
			b.Fatalf("Error receiving: %v", err)
		}
	}
}

func BenchmarkReceiveRequestNoBuf(b *testing.B) {
	req := MCRequest{
		Opcode:  SET,
		Cas:     0,
		Opaque:  7242,
		VBucket: 824,
	}.SetData([]byte{1}, []byte("somekey"), []byte("somevalue"))

	data := req.Bytes()
	data[0] = REQ_MAGIC
	rdr := bytes.NewReader(data)

	b.SetBytes(int64(len(data)))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rdr.Seek(0, 0)
		_, _, err := ReceiveRequest(rdr, nil)
		if err != nil {
			b.Fatalf("Error receiving: %v", err)
		}
	}
}

func TestReceivingTapRequest(t *testing.T) {
	content := []byte{
		REQ_MAGIC, byte(TAP_MUTATION),
		0x0, 0x7, // length of key
		0x2,       // extra length
		0x0,       // reserved
		0x3, 0x38, // vbucket
		0x0, 0x0, 0x0, 0x16, // Length of value
		0x0, 0x0, 0x1c, 0x4a, // opaque
		0x0, 0x0, 0x0, 0x0, 0x37, 0xef, 0x3a, 0x35, // CAS
		0, 4, // extra (describes length of engine specific
		1, 2, 3, 4, // engine specific junk
		's', 'o', 'm', 'e', 'k', 'e', 'y',
		's', 'o', 'm', 'e', 'v', 'a', 'l', 'u', 'e'}

	req, n, err := ReceiveRequest(bytes.NewReader(content), nil)
	if err != nil {
		t.Errorf("Failed to parse response.")
	}
	if n != len(content) {
		t.Errorf("Expected to read %v bytes, read %v", len(content), n)
	}

	exp := `{MCRequest opcode=TAP_MUTATION, bodylen=9, key='somekey'}`
	if req.String() != exp {
		t.Errorf("Expected string=%q, got %q", exp, req.String())
	}
}
