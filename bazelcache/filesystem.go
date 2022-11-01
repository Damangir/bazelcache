package bazelcache

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

const (
	permission    = 0700
	channelBuffer = 100
)

func ensureExist(dir string) error {
	_, err := os.Stat(dir)
	if os.IsNotExist(err) {
		// does not exist create one
		os.MkdirAll(dir, permission)
	} else if err != nil {
		return err
	}
	return nil
}

// NewFileSystemCache creates a fileSystemCache that backs its data to local
// file system. It deletes the least used files if the total size exceeds
// maxSize.
func NewFileSystemCache(cacheDir string, maxSize int) (*fileSystemCache, error) {
	ensureExist(cacheDir)
	type fileAndInfo struct {
		os.FileInfo
		path string
	}
	var files []fileAndInfo
	err := filepath.Walk(cacheDir,
		func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			// skip directories
			if info.IsDir() {
				return nil
			}
			files = append(files, fileAndInfo{
				FileInfo: info,
				path:     path,
			})
			return nil
		})
	if err != nil {
		return nil, fmt.Errorf("cannot extract %q directory structyre: %v", cacheDir, err)
	}
	sort.Slice(files, func(i, j int) bool {
		return files[i].ModTime().After(files[j].ModTime())
	})

	fsc := &fileSystemCache{
		base:     cacheDir,
		MaxSize:  maxSize,
		store:    make(map[string]*finfo),
		toRemove: make(chan string, channelBuffer),
		toTouch:  make(chan string, channelBuffer),
	}
	for _, fi := range files {
		if fsc.total > fsc.MaxSize {
			log.Println("remove", fi.path, fi.ModTime())
			err := os.Remove(fi.path)
			if err != nil {
				return nil, fmt.Errorf("cannot construct cache from the content of %q: %v", cacheDir, err)
			}
			continue
		}
		log.Println("add", fi.path, fi.ModTime())
		key, err := filepath.Rel(cacheDir, fi.path)
		if err != nil {
			return nil, fmt.Errorf("cannot construct cache from the content of %q: %v", cacheDir, err)
		}
		fsc.insert(&finfo{
			key:  key,
			size: int(fi.Size()),
		})
	}
	go fsc.housekeeper()
	return fsc, nil
}

type fileSystemCache struct {
	MaxSize  int
	base     string
	last     *finfo
	first    *finfo
	store    map[string]*finfo
	total    int
	mx       sync.Mutex
	toRemove chan string
	toTouch  chan string
}

type finfo struct {
	key  string
	size int
	next *finfo
	prev *finfo
}

// Get retrieves the object with a given key
func (fs *fileSystemCache) Get(ctx context.Context, key string) ([]byte, error) {
	fs.mx.Lock()
	fi, ok := fs.store[key]
	if !ok {
		fs.mx.Unlock()
	} else {
		fs.putFirst(fi)
		fs.mx.Unlock()
		b, err := os.ReadFile(fs.fname(fi.key))
		if err == os.ErrNotExist {
			err = ErrNotFound
		}
		return b, err
	}
	return nil, ErrNotFound
}

// Put stores the object with a given key
func (fs *fileSystemCache) Put(ctx context.Context, key string, r io.Reader) (err error) {
	// return if we already stored this key.

	recordFile := fs.fname(key)
	if err := ensureExist(filepath.Dir(recordFile)); err != nil {
		return err
	}
	w, err := os.Create(recordFile)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			os.Remove(recordFile)
		}
	}()

	n, err := io.Copy(w, r)
	if err != nil {
		w.Close()
		return err
	}
	err = w.Close()
	if err != nil {
		return err
	}
	// the file is written, now update the store.
	fs.mx.Lock()
	fs.insert(&finfo{
		key:  key,
		size: int(n),
	})
	fs.mx.Unlock()
	return nil
}

// Delete deletes the object with a given key
func (fs *fileSystemCache) Delete(ctx context.Context, key string) error {
	fs.mx.Lock()
	toRemove, ok := fs.store[key]
	if !ok {
		fs.mx.Unlock()
		return nil
	}
	fs.remove(toRemove)
	fs.mx.Unlock()
	fs.rmFileForKey(toRemove.key)
	return nil
}

// Has checks wether an object with a given key exists.
func (fs *fileSystemCache) Has(ctx context.Context, key string) bool {
	fs.mx.Lock()
	defer fs.mx.Unlock()
	if _, ok := fs.store[key]; ok {
		return true
	}
	return false
}

func (fs *fileSystemCache) housekeeper() {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case name := <-fs.toRemove:
			os.Remove(name)
		case name := <-fs.toTouch:
			// touch the file
			currentTime := time.Now().Local()
			os.Chtimes(name, currentTime, currentTime)
		case <-ticker.C:
			fs.ensureSize()
		}
	}
}

func (fs *fileSystemCache) rmFileForKey(key string) {
	fs.toRemove <- fs.fname(key)
}

func (fs *fileSystemCache) touchFileForKey(key string) {
	fs.toTouch <- fs.fname(key)
}

func (fs *fileSystemCache) fname(key string) string {
	return path.Join(fs.base, key)
}

// remove the latest items until the total size is bellow MaxSize
func (fs *fileSystemCache) ensureSize() {
	for fs.total > fs.MaxSize {
		toRemove := fs.last
		fs.mx.Lock()
		fs.remove(toRemove)
		fs.mx.Unlock()
		fs.rmFileForKey(toRemove.key)
	}
}

// insert put an item in the begining of the list. It is not thread safe.
func (fs *fileSystemCache) insert(toInsert *finfo) {
	fs.store[toInsert.key] = toInsert
	toInsert.prev = fs.first
	if fs.first != nil {
		fs.first.next = toInsert
	}
	fs.first = toInsert
	if fs.last == nil {
		fs.last = toInsert
	}
	fs.total += toInsert.size
}

// remove drops an item from the list. It is not thread safe.
func (fs *fileSystemCache) remove(toRemove *finfo) {
	fs.total -= toRemove.size
	if toRemove.next != nil {
		toRemove.next.prev = toRemove.prev
	} else {
		// toRemove is first
		fs.first = toRemove.prev
	}
	if toRemove.prev != nil {
		toRemove.prev.next = toRemove.next
	} else {
		// toRemove is last
		fs.last = toRemove.next
	}
	delete(fs.store, toRemove.key)
}

// putFirst brings an item to the front of the list. It is not thread safe.
func (fs *fileSystemCache) putFirst(item *finfo) {
	if item.next == nil {
		// the item is already first
		return
	}
	item.next.prev = item.prev
	if item.prev != nil {
		item.prev.next = item.next
	}
	item.prev = fs.first
	fs.first.next = item
	fs.touchFileForKey(item.key)
}
