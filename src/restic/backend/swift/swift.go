package swift

import (
	"io"
	"path"
	"restic"
	"restic/debug"
	"restic/errors"
	"strings"
	"time"

	"github.com/ncw/swift"
)

const connLimit = 10

// beSwift is a backend which stores the data on a swift endpoint.
type beSwift struct {
	conn      *swift.Connection
	connChan  chan struct{}
	container string // Container name
	prefix    string // Prefix of object names in the container
}

// Open opens the swift backend at a container in region. The container is
// created if it does not exist yet.
func Open(cfg Config) (restic.Backend, error) {

	be := &beSwift{
		conn: &swift.Connection{
			UserName:       cfg.UserName,
			Domain:         cfg.Domain,
			ApiKey:         cfg.APIKey,
			AuthUrl:        cfg.AuthURL,
			Region:         cfg.Region,
			Tenant:         cfg.Tenant,
			TenantId:       cfg.TenantID,
			TenantDomain:   cfg.TenantDomain,
			TrustId:        cfg.TrustID,
			StorageUrl:     cfg.StorageURL,
			AuthToken:      cfg.AuthToken,
			ConnectTimeout: time.Minute,
			Timeout:        time.Minute,
		},
		container: cfg.Container,
		prefix:    cfg.Prefix,
	}
	be.createConnections()

	// Authenticate if needed
	if !be.conn.Authenticated() {
		if err := be.conn.Authenticate(); err != nil {
			return nil, errors.Wrap(err, "conn.Authenticate")
		}
	}

	// Ensure container exists
	switch _, _, err := be.conn.Container(be.container); err {
	case nil:
		// Container exists

	case swift.ContainerNotFound:
		err = be.createContainer(cfg.DefaultContainerPolicy)
		if err != nil {
			return nil, errors.Wrap(err, "beSwift.createContainer")
		}

	default:
		return nil, errors.Wrap(err, "conn.Container")
	}

	return be, nil
}

func (be *beSwift) swiftpath(t restic.FileType, name string) string {
	if t == restic.ConfigFile {
		return path.Join(be.prefix, string(t))
	}
	return path.Join(be.prefix, string(t), name)
}

func (be *beSwift) createConnections() {
	be.connChan = make(chan struct{}, connLimit)
	for i := 0; i < connLimit; i++ {
		be.connChan <- struct{}{}
	}
}

func (be *beSwift) createContainer(policy string) error {
	var h swift.Headers
	if policy != "" {
		h = swift.Headers{
			"X-Storage-Policy": policy,
		}
	}

	return be.conn.ContainerCreate(be.container, h)
}

// Location returns this backend's location (the container name).
func (be *beSwift) Location() string {
	return be.container
}

// Load returns the data stored in the backend for h at the given offset
// and saves it in p. Load has the same semantics as io.ReaderAt.
func (be *beSwift) Load(h restic.Handle, p []byte, off int64) (n int, err error) {
	if err := h.Valid(); err != nil {
		return 0, err
	}

	debug.Log("%v, offset %v, len %v", h, off, len(p))
	objName := be.swiftpath(h.Type, h.Name)

	<-be.connChan
	defer func() {
		be.connChan <- struct{}{}
	}()

	obj, _, err := be.conn.ObjectOpen(be.container, objName, false, nil)
	if err != nil {
		debug.Log("  err %v", err)
		return 0, errors.Wrap(err, "conn.ObjectOpen")
	}

	// make sure that the object is closed properly.
	defer func() {
		e := obj.Close()
		if err == nil {
			err = errors.Wrap(e, "obj.Close")
		}
	}()

	length, err := obj.Length()
	if err != nil {
		return 0, errors.Wrap(err, "obj.Length")
	}

	// handle negative offsets
	if off < 0 {
		// if the negative offset is larger than the object itself, read from
		// the beginning.
		if -off > length {
			off = 0
		} else {
			// otherwise compute the offset from the end of the file.
			off = length + off
		}
	}

	// return an error if the offset is beyond the end of the file
	if off > length {
		return 0, errors.Wrap(io.EOF, "")
	}

	var nextError error

	// manually create an io.ErrUnexpectedEOF
	if off+int64(len(p)) > length {
		newlen := length - off
		p = p[:newlen]

		nextError = io.ErrUnexpectedEOF

		debug.Log("    capped buffer to %v bytes", len(p))
	}

	_, err = obj.Seek(off, 0)
	if err != nil {
		return 0, errors.Wrap(err, "obj.Seek")
	}

	n, err = io.ReadFull(obj, p)
	if int64(n) == length-off && errors.Cause(err) == io.EOF {
		err = nil
	}

	if err == nil {
		err = nextError
	}

	return n, err
}

// Save stores data in the backend at the handle.
func (be *beSwift) Save(h restic.Handle, p []byte) (err error) {
	if err = h.Valid(); err != nil {
		return err
	}

	debug.Log("%v with %d bytes", h, len(p))

	objName := be.swiftpath(h.Type, h.Name)

	// Check key does not already exist
	switch _, _, err = be.conn.Object(be.container, objName); err {
	case nil:
		debug.Log("%v already exists", h)
		return errors.New("key already exists")

	case swift.ObjectNotFound:
		// Ok, that's what we want

	default:
		return errors.Wrap(err, "conn.Object")
	}

	<-be.connChan
	defer func() {
		be.connChan <- struct{}{}
	}()

	encoding := "binary/octet-stream"

	debug.Log("PutObject(%v, %v, %v, %v)",
		be.container, objName, int64(len(p)), encoding)
	err = be.conn.ObjectPutBytes(be.container, objName, p, encoding)
	debug.Log("%v -> %v bytes, err %#v", objName, len(p), err)

	return errors.Wrap(err, "client.PutObject")
}

// Stat returns information about a blob.
func (be *beSwift) Stat(h restic.Handle) (bi restic.FileInfo, err error) {
	debug.Log("%v", h)

	objName := be.swiftpath(h.Type, h.Name)

	obj, _, err := be.conn.Object(be.container, objName)
	if err != nil {
		debug.Log("Object() err %v", err)
		return restic.FileInfo{}, errors.Wrap(err, "conn.Object")
	}

	return restic.FileInfo{Size: obj.Bytes}, nil
}

// Test returns true if a blob of the given type and name exists in the backend.
func (be *beSwift) Test(t restic.FileType, name string) (bool, error) {

	objName := be.swiftpath(t, name)
	switch _, _, err := be.conn.Object(be.container, objName); err {
	case nil:
		return true, nil

	case swift.ObjectNotFound:
		return false, nil

	default:
		return false, errors.Wrap(err, "conn.Object")
	}
}

// Remove removes the blob with the given name and type.
func (be *beSwift) Remove(t restic.FileType, name string) error {
	objName := be.swiftpath(t, name)
	err := be.conn.ObjectDelete(be.container, objName)
	debug.Log("%v %v -> err %v", t, name, err)
	return errors.Wrap(err, "conn.ObjectDelete")
}

// List returns a channel that yields all names of blobs of type t. A
// goroutine is started for this. If the channel done is closed, sending
// stops.
func (be *beSwift) List(t restic.FileType, done <-chan struct{}) <-chan string {
	debug.Log("listing %v", t)
	ch := make(chan string)

	prefix := be.swiftpath(t, "") + "/"

	go func() {
		defer close(ch)

		be.conn.ObjectsWalk(be.container, &swift.ObjectsOpts{Prefix: prefix},
			func(opts *swift.ObjectsOpts) (interface{}, error) {
				newObjects, err := be.conn.ObjectNames(be.container, opts)
				if err != nil {
					return nil, errors.Wrap(err, "conn.ObjectNames")
				}
				for _, obj := range newObjects {
					m := strings.TrimPrefix(obj, prefix)
					if m == "" {
						continue
					}

					select {
					case ch <- m:
					case <-done:
						return nil, io.EOF
					}
				}
				return newObjects, nil
			})
	}()

	return ch
}

// Remove keys for a specified backend type.
func (be *beSwift) removeKeys(t restic.FileType) error {
	done := make(chan struct{})
	defer close(done)
	for key := range be.List(restic.DataFile, done) {
		err := be.Remove(restic.DataFile, key)
		if err != nil {
			return err
		}
	}

	return nil
}

// Delete removes all restic objects in the container.
// It will not remove the container itself.
func (be *beSwift) Delete() error {
	alltypes := []restic.FileType{
		restic.DataFile,
		restic.KeyFile,
		restic.LockFile,
		restic.SnapshotFile,
		restic.IndexFile}

	for _, t := range alltypes {
		err := be.removeKeys(t)
		if err != nil {
			return nil
		}
	}

	return be.Remove(restic.ConfigFile, "")
}

// Close does nothing
func (be *beSwift) Close() error { return nil }
