package hashmap

import (
	"context"
	"fmt"
	"io"
	"path"
	"strings"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/fs/operations"
)

// NewObject finds the Object at remote. If it can't be found, it returns the
// error ErrorObjectNotFound.
//
// If remote points to a directory then it should return ErrorIsDir if possible
// without doing any extra work, otherwise ErrorObjectNotFound.
func (f *Fs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	if _, ok := f.dirMap.Path[remote]; ok {
		return nil, fs.ErrorIsDir
	}
	_, base := path.Split(remote)
	entry, fileHash, ok := f.toHash(remote)
	if !ok {
		return nil, fs.ErrorObjectNotFound
	}
	files, err := entry.Files(ctx)
	if err != nil {
		return nil, err
	}
	if _, ok := files[base]; !ok {
		return nil, fs.ErrorObjectNotFound
	}
	basePath := path.Join(entry.Hash, fileHash)
	obj, err := f.base.NewObject(ctx, path.Join(basePath, "data"))
	if err != nil {
		return nil, fmt.Errorf("error fetching base object: %w", err)
	}
	return object{
		obj:      obj,
		path:     remote,
		basePath: basePath,
		fs:       f,
		dirEntry: entry,
	}, nil
}

// Put puts in to the remote path with the modTime given of the given size.
//
// When called from outside an Fs by rclone, src.Size() will always be >= 0.
// For unknown-sized objects (indicated by src.Size() == -1), Put should either
// return an error or upload it properly (rather than e.g. calling panic).
//
// This function may create the object even if it returns an error - if so will
// return the object and the error, otherwise will return nil and the error.
func (f *Fs) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	return f.put(ctx, f.base.Put, in, src, options...)
}

// PutStream is equivalent to Put except the file is of unknown size.
func (f *Fs) PutStream(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	return f.put(ctx, f.base.Features().PutStream, in, src, options...)
}

// PutUnchecked is equivalent to Put except there are no checks for duplicates.
func (f *Fs) PutUnchecked(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	return f.put(ctx, f.base.Features().PutUnchecked, in, src, options...)
}

// Copy copies the specified file to the specified path.
func (f *Fs) Copy(ctx context.Context, src fs.Object, remote string) (fs.Object, error) {
	do := f.base.Features().Copy
	if do == nil {
		return nil, fs.ErrorCantCopy
	}
	if strings.Contains(remote, "\n") {
		return nil, fmt.Errorf("file name may not contain newline: %q", src.Remote())
	}
	entry, fileHash, ok := f.toHash(remote)
	if !ok {
		return nil, fs.ErrorDirNotFound
	}
	if err := f.prepareDest(ctx, src, src.Remote(), entry.Hash, fileHash); err != nil {
		return nil, err
	}
	if err := entry.write(ctx); err != nil {
		return nil, err
	}
	return do(ctx, src.(object).UnWrap(), path.Join(entry.Hash, fileHash, "data"))
}

type putFn func(context.Context, io.Reader, fs.ObjectInfo, ...fs.OpenOption) (fs.Object, error)

func (f *Fs) put(ctx context.Context, do putFn, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	if strings.Contains(src.Remote(), "\n") {
		return nil, fmt.Errorf("file name may not contain newline: %q", src.Remote())
	}
	_, base := path.Split(src.Remote())
	entry, fileHash, ok := f.toHash(src.Remote())
	if !ok {
		return nil, fs.ErrorDirNotFound
	}
	if err := f.prepareDest(ctx, src, src.Remote(), entry.Hash, fileHash); err != nil {
		return nil, err
	}
	// Create the data file.
	dataSrc := fakeObjInfo{
		objInfo: src,
		remote:  path.Join(entry.Hash, fileHash, "data"),
		fs:      f,
	}
	obj, err := do(ctx, in, dataSrc, options...)
	if err != nil {
		return nil, fmt.Errorf("error creating data file: %w", err)
	}
	files, err := entry.Files(ctx)
	if err != nil {
		return nil, err
	}
	files[base] = fileHash
	// Wrap the object.
	obj = object{
		obj:      obj,
		path:     src.Remote(),
		basePath: path.Join(entry.Hash, fileHash),
		fs:       f,
		dirEntry: entry,
	}
	// Record the new file in the map.
	if err := entry.write(ctx); err != nil {
		return obj, err
	}
	return obj, nil
}

// prepareDest is a helper function that creates the directory structure for a
// given file creation. It does not create the "data" file.
func (f *Fs) prepareDest(ctx context.Context, src fs.ObjectInfo, overlay, dirHash, fileHash string) error {
	// Create the directory for the file.
	err := f.base.Mkdir(ctx, dirHash)
	if err != nil {
		return err
	}
	err = f.base.Mkdir(ctx, path.Join(dirHash, fileHash))
	if err != nil {
		return fmt.Errorf("error creating directory for file: %w", err)
	}
	// Create the name file.
	nameSrc := fakeObjInfo{
		objInfo: src,
		remote:  path.Join(dirHash, fileHash, "name"),
		fs:      f,
		size:    int64(len(overlay) + 1),
	}
	_, err = f.base.Put(ctx, strings.NewReader(overlay+"\n"), nameSrc)
	if err != nil {
		return fmt.Errorf("error creating name file: %w", err)
	}
	return nil
}

var (
	// TODO:
	// var _ fs.FullObject = object{}
	_ fs.Object     = object{}
	_ fs.MimeTyper  = object{}
	_ fs.ObjectInfo = fakeObjInfo{}
)

// object is an implementation of DirEntry that represents an object.
type object struct {
	// obj is the underlying data file representing the object.
	obj fs.Object
	// path is the path of the object.
	path string
	// basePath is the base path of the object.
	basePath string
	// fs is the Fs that created the object.
	fs *Fs
	// dirEntry is the directory that the object belongs to.
	dirEntry *dirEntry
}

// String returns the string representation of the object.
func (o object) String() string {
	return o.path
}

// Remote returns the actual path of the object.
func (o object) Remote() string {
	return o.path
}

// ModTime returns the modification time as reported by the base object.
func (o object) ModTime(ctx context.Context) time.Time {
	return o.obj.ModTime(ctx)
}

// Size returns the size as reported by the base object.
func (o object) Size() int64 {
	return o.obj.Size()
}

// ID returns the ID from the base FS or an empty string if the base object
// does not implement IDer.
func (o object) ID() string {
	if ider, ok := o.obj.(fs.IDer); ok {
		return ider.ID()
	}
	return ""
}

// Fs returns the Fs that created the object.
func (o object) Fs() fs.Info {
	return o.fs
}

// Hash returns the selected checksum of the file. If no checksum is available,
// it returns "".
func (o object) Hash(ctx context.Context, ty hash.Type) (string, error) {
	return o.obj.Hash(ctx, ty)
}

// Storable returns if the object can be stored.
func (o object) Storable() bool {
	return o.obj.Storable()
}

// SetModTime sets the modification time of the base object.
func (o object) SetModTime(ctx context.Context, t time.Time) error {
	return o.obj.SetModTime(ctx, t)
}

// Open opens the file for read.  Call Close() on the returned io.ReadCloser
func (o object) Open(ctx context.Context, options ...fs.OpenOption) (io.ReadCloser, error) {
	return o.obj.Open(ctx, options...)
}

// Update in to the object with the modTime given of the given size
//
// When called from outside an Fs by rclone, src.Size() will always be >= 0.
// But for unknown-sized objects (indicated by src.Size() == -1), Upload should
// either return an error or update the object properly (rather than e.g.
// calling panic).
func (o object) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {
	return o.obj.Update(ctx, in, src, options...)
}

// Remove removes the object and metadata associated with it.
func (o object) Remove(ctx context.Context) error {
	err := operations.Purge(ctx, o.fs.base, o.basePath)
	if err != nil {
		return err
	}
	files, err := o.dirEntry.Files(ctx)
	if err != nil {
		return fmt.Errorf("refusing to modify map file: cannot load map file: %w", err)
	}
	delete(files, path.Base(o.path))
	return o.dirEntry.write(ctx)
}

// UnWrap returns the "data" file of the Object.
func (o object) UnWrap() fs.Object {
	return o.obj
}

// MimeType returns the MIME type of the base object.
func (o object) MimeType(ctx context.Context) string {
	if mimeTyper, ok := o.obj.(fs.MimeTyper); ok {
		return mimeTyper.MimeType(ctx)
	}
	return ""
}

// fakeObjInfo is a ObjectInfo with its path faked to be used in
// implementations of Put.
type fakeObjInfo struct {
	objInfo fs.ObjectInfo
	remote  string
	fs      *Fs
	size    int64
}

// String returns the string representation of the object info.
func (f fakeObjInfo) String() string {
	if f.objInfo == nil {
		return fmt.Sprintf("<fakeObjInfo: nil, %q, %d>", f.remote, f.size)
	}
	return f.objInfo.String()
}

// Fs returns the Fs wrapping the base FS.
func (f fakeObjInfo) Fs() fs.Info {
	return f.fs
}

// Remote returns the faked remote path.
func (f fakeObjInfo) Remote() string {
	return f.remote
}

// ModTime returns the intended modification time of the object info. It passes
// on the underlying object info or return the current time.
func (f fakeObjInfo) ModTime(ctx context.Context) time.Time {
	if f.objInfo == nil {
		return time.Now()
	}
	return f.objInfo.ModTime(ctx)
}

// Size returns the faked size if it is set (non-zero) and the base object's
// size otherwise.
func (f fakeObjInfo) Size() int64 {
	if f.size != 0 {
		return f.size
	}
	return f.objInfo.Size()
}

// Hash returns the Hash from the base object info or an error.
func (f fakeObjInfo) Hash(ctx context.Context, ty hash.Type) (string, error) {
	if f.objInfo == nil {
		return "", fmt.Errorf("hash not available in fakeObjInfo")
	}
	return f.objInfo.Hash(ctx, ty)
}

// Storable returns if the base object info is storable or true.
func (f fakeObjInfo) Storable() bool {
	if f.objInfo == nil {
		return true
	}
	return f.objInfo.Storable()
}
