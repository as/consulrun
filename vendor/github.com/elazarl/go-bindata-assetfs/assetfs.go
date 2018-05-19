package assetfs

import (
	"bytes"
	"errors"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"time"
)

var (
	defaultFileTimestamp = time.Now()
)

type FakeFile struct {
	Path string

	Dir bool

	Len int64

	Timestamp time.Time
}

func (f *FakeFile) Name() string {
	_, name := filepath.Split(f.Path)
	return name
}

func (f *FakeFile) Mode() os.FileMode {
	mode := os.FileMode(0644)
	if f.Dir {
		return mode | os.ModeDir
	}
	return mode
}

func (f *FakeFile) ModTime() time.Time {
	return f.Timestamp
}

func (f *FakeFile) Size() int64 {
	return f.Len
}

func (f *FakeFile) IsDir() bool {
	return f.Mode().IsDir()
}

func (f *FakeFile) Sys() interface{} {
	return nil
}

type AssetFile struct {
	*bytes.Reader
	io.Closer
	FakeFile
}

func NewAssetFile(name string, content []byte, timestamp time.Time) *AssetFile {
	if timestamp.IsZero() {
		timestamp = defaultFileTimestamp
	}
	return &AssetFile{
		bytes.NewReader(content),
		ioutil.NopCloser(nil),
		FakeFile{name, false, int64(len(content)), timestamp}}
}

func (f *AssetFile) Readdir(count int) ([]os.FileInfo, error) {
	return nil, errors.New("not a directory")
}

func (f *AssetFile) Size() int64 {
	return f.FakeFile.Size()
}

func (f *AssetFile) Stat() (os.FileInfo, error) {
	return f, nil
}

type AssetDirectory struct {
	AssetFile
	ChildrenRead int
	Children     []os.FileInfo
}

func NewAssetDirectory(name string, children []string, fs *AssetFS) *AssetDirectory {
	fileinfos := make([]os.FileInfo, 0, len(children))
	for _, child := range children {
		_, err := fs.AssetDir(filepath.Join(name, child))
		fileinfos = append(fileinfos, &FakeFile{child, err == nil, 0, time.Time{}})
	}
	return &AssetDirectory{
		AssetFile{
			bytes.NewReader(nil),
			ioutil.NopCloser(nil),
			FakeFile{name, true, 0, time.Time{}},
		},
		0,
		fileinfos}
}

func (f *AssetDirectory) Readdir(count int) ([]os.FileInfo, error) {
	if count <= 0 {
		return f.Children, nil
	}
	if f.ChildrenRead+count > len(f.Children) {
		count = len(f.Children) - f.ChildrenRead
	}
	rv := f.Children[f.ChildrenRead : f.ChildrenRead+count]
	f.ChildrenRead += count
	return rv, nil
}

func (f *AssetDirectory) Stat() (os.FileInfo, error) {
	return f, nil
}

type AssetFS struct {
	Asset func(path string) ([]byte, error)

	AssetDir func(path string) ([]string, error)

	AssetInfo func(path string) (os.FileInfo, error)

	Prefix string
}

func (fs *AssetFS) Open(name string) (http.File, error) {
	name = path.Join(fs.Prefix, name)
	if len(name) > 0 && name[0] == '/' {
		name = name[1:]
	}
	if b, err := fs.Asset(name); err == nil {
		timestamp := defaultFileTimestamp
		if info, err := fs.AssetInfo(name); err == nil {
			timestamp = info.ModTime()
		}
		return NewAssetFile(name, b, timestamp), nil
	}
	if children, err := fs.AssetDir(name); err == nil {
		return NewAssetDirectory(name, children, fs), nil
	} else {
		return nil, err
	}
}
