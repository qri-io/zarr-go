package zarr

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
)

const (
	MemoryStoreType   = "MemoryStore"
	LocalStoreType    = "LocalStore"
	dirPermissionBits = 0644
)

var ErrNotfound = errors.New("not found")

type Store interface {
	Get(key string) (io.ReadCloser, error)
	Put(key string, val io.Reader) error
	Type() string
}

type MemoryStore struct {
	lk   sync.Mutex
	data map[string][]byte
}

var _ Store = (*MemoryStore)(nil)

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		data: map[string][]byte{},
	}
}

func (s *MemoryStore) Type() string { return MemoryStoreType }

func (s *MemoryStore) Get(key string) (io.ReadCloser, error) {
	s.lk.Lock()
	defer s.lk.Unlock()
	d, ok := s.data[key]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrNotfound, key)
	}
	return ioutil.NopCloser(bytes.NewBuffer(d)), nil
}

func (s *MemoryStore) Put(key string, val io.Reader) error {
	d, err := ioutil.ReadAll(val)
	if err != nil {
		return err
	}

	s.lk.Lock()
	defer s.lk.Unlock()
	s.data[key] = d

	return nil
}

type LocalStore struct {
	base string
}

var _ Store = (*LocalStore)(nil)

func NewLocalStore(base string) (*LocalStore, error) {
	base, err := filepath.Abs(base)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(base, dirPermissionBits); err != nil {
		return nil, err
	}

	return &LocalStore{
		base: base,
	}, nil
}

func (s *LocalStore) Type() string { return LocalStoreType }

func (s *LocalStore) Get(key string) (io.ReadCloser, error) {
	return os.Open(filepath.Join(s.base, key))
}

func (s *LocalStore) Put(key string, val io.Reader) error {
	path := filepath.Join(s.base, key)
	if err := os.MkdirAll(filepath.Dir(path), dirPermissionBits); err != nil {
		return err
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}

	io.Copy(f, val)
	if c, ok := val.(io.Closer); ok {
		return c.Close()
	}
	return nil
}
