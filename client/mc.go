// Package memcached provides a memcached binary protocol client.
package memcached

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"strings"
	"time"

	"github.com/dustin/gomemcached"
)

const bufsize = 1024

// The Client itself.
type Client struct {
	conn    io.ReadWriteCloser
	healthy bool

	hdrBuf []byte
}

// Connect to a memcached server.
func Connect(prot, dest string) (rv *Client, err error) {
	conn, err := net.Dial(prot, dest)
	if err != nil {
		return nil, err
	}
	return Wrap(conn)
}

// Wrap an existing transport.
func Wrap(rwc io.ReadWriteCloser) (rv *Client, err error) {
	return &Client{
		conn:    rwc,
		healthy: true,
		hdrBuf:  make([]byte, gomemcached.HDR_LEN),
	}, nil
}

// Close the connection when you're done.
func (c *Client) Close() {
	c.conn.Close()
}

// IsHealthy returns true unless the client is belived to have
// difficulty communicating to its server.
//
// This is useful for connection pools where we want to
// non-destructively determine that a connection may be reused.
func (c Client) IsHealthy() bool {
	return c.healthy
}

// Send a custom request and get the response.
func (client *Client) Send(req *gomemcached.MCRequest) (rv *gomemcached.MCResponse, err error) {
	err = transmitRequest(client.conn, req)
	if err != nil {
		client.healthy = false
		return
	}
	resp, err := getResponse(client.conn, client.hdrBuf)
	client.healthy = !gomemcached.IsFatal(err)
	return resp, err
}

// Transmit send a request, but does not wait for a response.
func (client *Client) Transmit(req *gomemcached.MCRequest) error {
	err := transmitRequest(client.conn, req)
	if err != nil {
		client.healthy = false
	}
	return err
}

// Receive a response
func (client *Client) Receive() (*gomemcached.MCResponse, error) {
	resp, err := getResponse(client.conn, client.hdrBuf)
	if err != nil {
		client.healthy = false
	}
	return resp, err
}

// Get the value for a key.
func (client *Client) Get(vb uint16, key string) (*gomemcached.MCResponse, error) {
	return client.Send(&gomemcached.MCRequest{
		Opcode:  gomemcached.GET,
		VBucket: vb,
		Key:     []byte(key),
	})
}

// Del deletes a key.
func (client *Client) Del(vb uint16, key string) (*gomemcached.MCResponse, error) {
	return client.Send(&gomemcached.MCRequest{
		Opcode:  gomemcached.DELETE,
		VBucket: vb,
		Key:     []byte(key)})
}

// AuthList lists SASL auth mechanisms.
func (client *Client) AuthList() (*gomemcached.MCResponse, error) {
	return client.Send(&gomemcached.MCRequest{
		Opcode: gomemcached.SASL_LIST_MECHS})
}

// Auth performs SASL PLAIN authentication against the server.
func (client *Client) Auth(user, pass string) (*gomemcached.MCResponse, error) {
	res, err := client.AuthList()

	if err != nil {
		return res, err
	}

	authMech := string(res.Body)
	if strings.Index(authMech, "PLAIN") != -1 {
		return client.Send(&gomemcached.MCRequest{
			Opcode: gomemcached.SASL_AUTH,
			Key:    []byte("PLAIN"),
			Body:   []byte(fmt.Sprintf("\x00%s\x00%s", user, pass))})
	}
	return res, fmt.Errorf("Auth mechanism PLAIN not supported")
}

func (client *Client) store(opcode gomemcached.CommandCode, vb uint16,
	key string, flags int, exp int, body []byte) (*gomemcached.MCResponse, error) {

	req := &gomemcached.MCRequest{
		Opcode:  opcode,
		VBucket: vb,
		Key:     []byte(key),
		Cas:     0,
		Opaque:  0,
		Extras:  []byte{0, 0, 0, 0, 0, 0, 0, 0},
		Body:    body}

	binary.BigEndian.PutUint64(req.Extras, uint64(flags)<<32|uint64(exp))
	return client.Send(req)
}

// Incr increments the value at the given key.
func (client *Client) Incr(vb uint16, key string,
	amt, def uint64, exp int) (uint64, error) {

	req := &gomemcached.MCRequest{
		Opcode:  gomemcached.INCREMENT,
		VBucket: vb,
		Key:     []byte(key),
		Extras:  make([]byte, 8+8+4),
	}
	binary.BigEndian.PutUint64(req.Extras[:8], amt)
	binary.BigEndian.PutUint64(req.Extras[8:16], def)
	binary.BigEndian.PutUint32(req.Extras[16:20], uint32(exp))

	resp, err := client.Send(req)
	if err != nil {
		return 0, err
	}

	return binary.BigEndian.Uint64(resp.Body), nil
}

// Add a value for a key (store if not exists).
func (client *Client) Add(vb uint16, key string, flags int, exp int,
	body []byte) (*gomemcached.MCResponse, error) {
	return client.store(gomemcached.ADD, vb, key, flags, exp, body)
}

// Set the value for a key.
func (client *Client) Set(vb uint16, key string, flags int, exp int,
	body []byte) (*gomemcached.MCResponse, error) {
	return client.store(gomemcached.SET, vb, key, flags, exp, body)
}

// Append data to the value of a key.
func (client *Client) Append(vb uint16, key string, data []byte) (*gomemcached.MCResponse, error) {
	req := &gomemcached.MCRequest{
		Opcode:  gomemcached.APPEND,
		VBucket: vb,
		Key:     []byte(key),
		Cas:     0,
		Opaque:  0,
		Body:    data}

	return client.Send(req)
}

// GetBulk gets keys in bulk
func (client *Client) GetBulk(vb uint16, keys []string) (map[string]*gomemcached.MCResponse, error) {
	terminalOpaque := uint32(len(keys) + 5)
	rv := map[string]*gomemcached.MCResponse{}
	going := true

	defer func() {
		going = false
	}()

	errch := make(chan error, 2)

	go func() {
		defer func() { errch <- nil }()
		for going {
			res, err := client.Receive()
			if err != nil {
				errch <- err
				return
			}
			if res.Opaque == terminalOpaque {
				return
			}
			if res.Opcode != gomemcached.GETQ {
				log.Panicf("Unexpected opcode in GETQ response: %+v",
					res)
			}
			rv[keys[res.Opaque]] = res
		}
	}()

	for i, k := range keys {
		err := client.Transmit(&gomemcached.MCRequest{
			Opcode:  gomemcached.GETQ,
			VBucket: vb,
			Key:     []byte(k),
			Opaque:  uint32(i),
		})
		if err != nil {
			return rv, err
		}
	}

	err := client.Transmit(&gomemcached.MCRequest{
		Opcode: gomemcached.NOOP,
		Opaque: terminalOpaque})
	if err != nil {
		return rv, err
	}

	return rv, <-errch
}

// ObservedStatus is the type reported by the Observe method
type ObservedStatus uint8

const (
	ObservedNotPersisted     = ObservedStatus(0x00) // found, not persisted
	ObservedPersisted        = ObservedStatus(0x01) // found, persisted
	ObservedNotFound         = ObservedStatus(0x80) // not found (or a persisted delete)
	ObservedLogicallyDeleted = ObservedStatus(0x81) // pending deletion (not persisted yet)
)

// ObserveResult represents the data obtained by an Observe call
type ObserveResult struct {
	Status          ObservedStatus // Whether the value has been persisted/deleted
	Cas             uint64         // Current value's CAS
	PersistenceTime time.Duration  // Node's average time to persist a value
	ReplicationTime time.Duration  // Node's average time to replicate a value
}

// Observe gets the persistence/replication/CAS state of a key
func (client *Client) Observe(vb uint16, key string) (result ObserveResult, err error) {
	// http://www.couchbase.com/wiki/display/couchbase/Observe
	body := make([]byte, 4+len(key))
	binary.BigEndian.PutUint16(body[0:2], vb)
	binary.BigEndian.PutUint16(body[2:4], uint16(len(key)))
	copy(body[4:4+len(key)], key)

	res, err := client.Send(&gomemcached.MCRequest{
		Opcode:  gomemcached.OBSERVE,
		VBucket: vb,
		Body:    body,
	})
	if err != nil {
		return
	}

	// Parse the response data from the body:
	if len(res.Body) < 2+2+1 {
		err = io.ErrUnexpectedEOF
		return
	}
	outVb := binary.BigEndian.Uint16(res.Body[0:2])
	keyLen := binary.BigEndian.Uint16(res.Body[2:4])
	if len(res.Body) < 2+2+int(keyLen)+1+8 {
		err = io.ErrUnexpectedEOF
		return
	}
	outKey := string(res.Body[4 : 4+keyLen])
	if outVb != vb || outKey != key {
		err = fmt.Errorf("Observe returned wrong vbucket/key: %d/%q", outVb, outKey)
		return
	}
	result.Status = ObservedStatus(res.Body[4+keyLen])
	result.Cas = binary.BigEndian.Uint64(res.Body[5+keyLen:])
	// The response reuses the Cas field to store time statistics:
	result.PersistenceTime = time.Duration(res.Cas>>32) * time.Millisecond
	result.ReplicationTime = time.Duration(res.Cas&math.MaxUint32) * time.Millisecond
	return
}

// CheckPersistence checks whether a stored value has been persisted to disk yet.
func (result ObserveResult) CheckPersistence(cas uint64, deletion bool) (persisted bool, overwritten bool) {
	switch {
	case result.Status == ObservedNotFound && deletion:
		persisted = true
	case result.Cas != cas:
		overwritten = true
	case result.Status == ObservedPersisted:
		persisted = true
	}
	return
}

// CasOp is the type of operation to perform on this CAS loop.
type CasOp uint8

const (
	// CASStore instructs the server to store the new value normally
	CASStore = CasOp(iota)
	// CASQuit instructs the client to stop attempting to CAS, leaving value untouched
	CASQuit
	// CASDelete instructs the server to delete the current value
	CASDelete
)

// User specified termination is returned as an error.
func (c CasOp) Error() string {
	switch c {
	case CASStore:
		return "CAS store"
	case CASQuit:
		return "CAS quit"
	case CASDelete:
		return "CAS delete"
	}
	panic("Unhandled value")
}

//////// CAS TRANSFORM

// CASState tracks the state of CAS over several operations.
//
// This is used directly by CASNext and indirectly by CAS
type CASState struct {
	initialized bool   // false on the first call to CASNext, then true
	Value       []byte // Current value of key; update in place to new value
	Cas         uint64 // Current CAS value of key
	Exists      bool   // Does a value exist for the key? (If not, Value will be nil)
	Err         error  // Error, if any, after CASNext returns false
	resp        *gomemcached.MCResponse
}

// CASNext is a non-callback, loop-based version of CAS method.
//
//  Usage is like this:
//
// var state memcached.CASState
// for client.CASNext(vb, key, exp, &state) {
//     state.Value = some_mutation(state.Value)
// }
// if state.Err != nil { ... }
func (client *Client) CASNext(vb uint16, k string, exp int, state *CASState) bool {
	if state.initialized {
		if !state.Exists {
			// Adding a new key:
			if state.Value == nil {
				state.Cas = 0
				return false // no-op (delete of non-existent value)
			}
			state.resp, state.Err = client.Add(vb, k, 0, exp, state.Value)
		} else {
			// Updating / deleting a key:
			req := &gomemcached.MCRequest{
				Opcode:  gomemcached.DELETE,
				VBucket: vb,
				Key:     []byte(k),
				Cas:     state.Cas}
			if state.Value != nil {
				req.Opcode = gomemcached.SET
				req.Opaque = 0
				req.Extras = []byte{0, 0, 0, 0, 0, 0, 0, 0}
				req.Body = state.Value

				flags := 0
				exp := 0 // ??? Should we use initialexp here instead?
				binary.BigEndian.PutUint64(req.Extras, uint64(flags)<<32|uint64(exp))
			}
			state.resp, state.Err = client.Send(req)
		}

		// If the response status is KEY_EEXISTS or NOT_STORED there's a conflict and we'll need to
		// get the new value (below). Otherwise, we're done (either success or failure) so return:
		if !(state.resp != nil && (state.resp.Status == gomemcached.KEY_EEXISTS ||
			state.resp.Status == gomemcached.NOT_STORED)) {
			state.Cas = state.resp.Cas
			return false // either success or fatal error
		}
	}

	// Initial call, or after a conflict: GET the current value and CAS and return them:
	state.initialized = true
	if state.resp, state.Err = client.Get(vb, k); state.Err == nil {
		state.Exists = true
		state.Value = state.resp.Body
		state.Cas = state.resp.Cas
	} else if state.resp != nil && state.resp.Status == gomemcached.KEY_ENOENT {
		state.Err = nil
		state.Exists = false
		state.Value = nil
		state.Cas = 0
	} else {
		return false // fatal error
	}
	return true // keep going...
}

// CasFunc is type type of function to perform a CAS transform.
//
// Input is the current value, or nil if no value exists.
// The function should return the new value (if any) to set, and the store/quit/delete operation.
type CasFunc func(current []byte) ([]byte, CasOp)

// CAS performs a CAS transform with the given function.
//
// If the value does not exist, a nil current value will be sent to f.
func (client *Client) CAS(vb uint16, k string, f CasFunc,
	initexp int) (*gomemcached.MCResponse, error) {
	var state CASState
	for client.CASNext(vb, k, initexp, &state) {
		newValue, operation := f(state.Value)
		if operation == CASQuit || (operation == CASDelete && state.Value == nil) {
			return nil, operation
		}
		state.Value = newValue
	}
	return state.resp, state.Err
}

// StatValue is one of the stats returned from the Stats method.
type StatValue struct {
	// The stat key
	Key string
	// The stat value
	Val string
}

// Stats requests server-side stats.
//
// use "" as the stat key for toplevel stats.
func (client *Client) Stats(key string) ([]StatValue, error) {
	rv := make([]StatValue, 0, 128)

	req := &gomemcached.MCRequest{
		Opcode: gomemcached.STAT,
		Key:    []byte(key),
		Opaque: 918494,
	}

	err := transmitRequest(client.conn, req)
	if err != nil {
		return rv, err
	}

	for {
		res, err := getResponse(client.conn, client.hdrBuf)
		if err != nil {
			return rv, err
		}
		k := string(res.Key)
		if k == "" {
			break
		}
		rv = append(rv, StatValue{
			Key: k,
			Val: string(res.Body),
		})
	}

	return rv, nil
}

// StatsMap requests server-side stats similarly to Stats, but returns
// them as a map.
func (client *Client) StatsMap(key string) (map[string]string, error) {
	rv := make(map[string]string)
	st, err := client.Stats(key)
	if err != nil {
		return rv, err
	}
	for _, sv := range st {
		rv[sv.Key] = sv.Val
	}
	return rv, nil
}
