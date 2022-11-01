package bazelcache

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log"
	"net/http"
	"strings"
)

var ErrNotFound = errors.New("resource was not found")

type BazelCache interface {
	// Get retrieves the object with a given key
	Get(ctx context.Context, key string) ([]byte, error)
	// Put stores the object with a given key
	Put(ctx context.Context, key string, r io.Reader) error
	// Delete deletes the object with a given key
	Delete(ctx context.Context, key string) error
	// Has checks wether an object with a given key exists.
	Has(ctx context.Context, key string) bool
}

type chainCache struct {
	bcs []BazelCache
}

// NewChainCache provides a chained cache. On write it tries to write to all
// underlying caches and on read it returns the first cache hit.
func NewChainCache(bcs ...BazelCache) BazelCache {
	return chainCache{bcs}
}

// Get retrieves the object with a given key from the first available cache.
func (cc chainCache) Get(ctx context.Context, key string) (out []byte, err error) {
	for _, bc := range cc.bcs {
		if out, err = bc.Get(ctx, key); err == nil {
			return out, nil
		}
	}
	return nil, err
}

// Put stores the object with a given key, returns success if at least one of
// the underlying cache suceesfully store the value.
func (cc chainCache) Put(ctx context.Context, key string, r io.Reader) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	for _, bc := range cc.bcs {
		if err1 := bc.Put(ctx, key, bytes.NewReader(data)); err1 == nil {
			err = nil
		}
	}
	return err
}

// Delete deletes the object with a given key from all the cache
func (cc chainCache) Delete(ctx context.Context, key string) error {
	for _, bc := range cc.bcs {
		bc.Delete(ctx, key)
	}
	return nil
}

// Has checks wether an object with a given key exists in any of the caches.
func (cc chainCache) Has(ctx context.Context, key string) bool {
	for _, bc := range cc.bcs {
		if bc.Has(ctx, key) {
			return true
		}
	}
	return false
}

type cacheServer struct {
	bc     BazelCache
	logger *log.Logger
}

func (cs cacheServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	key := strings.Trim(r.URL.Path, "/")
	w.Header().Set("Allow", "GET, PUT, HEAD, DELETE")
	status := http.StatusOK
	defer func() { cs.logger.Printf("[%s] %s [%v]", r.Method, key, status) }()
	switch r.Method {
	case http.MethodGet:
		out, err := cs.bc.Get(r.Context(), key)
		if err != nil {
			if err == ErrNotFound {
				status = http.StatusNotFound
			} else {
				status = http.StatusInternalServerError
			}
		} else {
			w.Write(out)
			return
		}

	case http.MethodPut:
		if err := cs.bc.Put(r.Context(), key, r.Body); err != nil {
			cs.logger.Print(err)
			status = http.StatusInternalServerError
		}

	case http.MethodDelete:
		if err := cs.bc.Delete(r.Context(), key); err != nil {
			status = http.StatusInternalServerError
		}

	case http.MethodHead:
		if cs.bc.Has(r.Context(), key) {
			status = http.StatusFound
		} else {
			status = http.StatusNotFound
		}

	default:
		status = http.StatusMethodNotAllowed
	}
	w.WriteHeader(status)
}

// NewCacheServer converts a BazelCache object to a handler compatible with
// Bazel remote cahce.
func NewCacheServer(bc BazelCache) http.Handler {
	return cacheServer{bc, log.Default()}
}
