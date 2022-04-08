package hashmap

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/operations"
)

// List the objects and directories in dir into entries. The entries can be
// returned in any order but should be for a complete directory.
//
// dir should be "" to list the root, and should not have trailing slashes.
//
// This should return ErrorDirNotFound if the directory isn't found.
func (f *Fs) List(ctx context.Context, dir string) (fs.DirEntries, error) {
	dir = path.Join(f.root, dir)
	entry, ok := f.dirMap.Path[dir]
	if !ok {
		return nil, fs.ErrorDirNotFound
	}
	return f.list(ctx, entry)
}

// ListR lists the objects and directories of the Fs starting from dir
// recursively into out.
//
// This should return ErrDirNotFound if the directory isn't found.
//
// It should call callback for each tranche of entries read. These need not be
// returned in any particular order.  If callback returns an error then the
// listing will stop immediately.
func (f *Fs) ListR(ctx context.Context, dir string, callback fs.ListRCallback) error {
	dir = path.Join(f.root, dir)
	entry, ok := f.dirMap.Path[dir]
	if !ok {
		return fs.ErrorDirNotFound
	}
	var recurse func(e *dirEntry) error
	recurse = func(e *dirEntry) error {
		entries, err := f.list(ctx, e)
		if err != nil {
			return err
		}
		if err := callback(entries); err != nil {
			return err
		}
		for _, child := range e.Children {
			if err := recurse(child); err != nil {
				return err
			}
		}
		return nil
	}
	return recurse(entry)
}

// list lists all the files in the given directory entry in a format rclone
// recognizes.
func (f *Fs) list(ctx context.Context, entry *dirEntry) (fs.DirEntries, error) {
	// List directories and files.
	subdirNames := make(map[string]*dirEntry, len(entry.Children))
	for _, child := range entry.Children {
		subdirNames[child.Hash] = child
	}
	files, err := entry.Files(ctx)
	if err != nil {
		return nil, err
	}
	subpathNames := make(map[string]struct{}, len(files))
	for path := range files {
		subpathNames[path] = struct{}{}
	}
	// Create fs.DirEntry.
	entries := make(fs.DirEntries, 0, len(entry.Children)+len(files))
	if len(subdirNames) > 0 {
		// Locate the entries from base.
		baseEntries, err := f.base.List(ctx, "")
		if err != nil {
			return nil, err
		}
		baseEntries.ForDir(func(d fs.Directory) {
			entry, ok := subdirNames[d.Remote()]
			if !ok {
				return
			}
			entries = append(entries, directory{
				dir:   d,
				entry: entry,
			})
			delete(subdirNames, d.Remote())
		})
	}
	for _, v := range subdirNames {
		// Equivalent remote directories were not found. List them regardless.
		entries = append(entries, directory{
			entry: v,
		})
	}
	for k := range subpathNames {
		filePath := path.Join(entry.Path, k)
		// Make path relative to root of FS.
		filePath = strings.TrimPrefix(filePath, f.root)
		filePath = strings.TrimPrefix(filePath, "/")
		obj, err := f.NewObject(ctx, filePath)
		if err != nil {
			// Don't fail listing due to one file. Just report a warning and
			// drop the file.
			fs.LogPrintf(fs.LogLevelWarning, obj, "error fetching object %q: %v", filePath, err)
			continue
		}
		entries = append(entries, obj)
	}
	return entries, nil
}

// Mkdir makes the specified directory. It should not return an error if it
// already exists.
func (f *Fs) Mkdir(ctx context.Context, dir string) error {
	dir = path.Join(f.root, dir)
	if _, ok := f.dirMap.Path[dir]; ok {
		return nil
	}
	if strings.Contains(dir, "\n") {
		return fmt.Errorf("directory name may not contain newline: %q", dir)
	}
	f.dirMap.newDirEntry(dir)
	entry := f.dirMap.Path[dir]
	if f.base.Features().CanHaveEmptyDirectories {
		err := f.base.Mkdir(ctx, entry.Hash)
		if err != nil {
			return err
		}
	}
	return f.dirMap.write(ctx)
}

// Rmdir removes the specified directory. It should return an error if the
// directory is not empty or it does not exist.
func (f *Fs) Rmdir(ctx context.Context, dir string) error {
	dir = path.Join(f.root, dir)
	entry, ok := f.dirMap.Path[dir]
	if !ok {
		return fs.ErrorDirNotFound
	}
	files, err := entry.Files(ctx)
	if err != nil {
		return fmt.Errorf("directory in a bad state, refusing to modify: %w", err)
	}
	if len(files) > 0 || len(entry.Children) > 0 {
		return fs.ErrorDirectoryNotEmpty
	}
	f.dirMap.removeEntry(dir)
	err = f.dirMap.write(ctx)
	if err != nil {
		return err
	}
	err = operations.Purge(ctx, f.base, entry.Hash)
	if errors.Is(err, fs.ErrorDirNotFound) {
		return nil
	}
	return err
}

// ChangeNotify invokes notify with the overlayed path when it receives a
// notification from the base FS.
func (f *Fs) ChangeNotify(ctx context.Context, notify func(string, fs.EntryType), interval <-chan time.Duration) {
	do := f.base.Features().ChangeNotify
	if do == nil {
		return
	}
	wrappedNotify := func(path string, typ fs.EntryType) {
		split := strings.Split(path, "/")
		if split[len(split)-1] != "data" {
			// Fire on "data" file modification only.
			return
		}
		if len(split) < 3 {
			// Something is wrong with this event. Skip it.
			return
		}
		dirHash := strings.Join(split[:len(split)-2], "/")
		fileHash := split[len(split)-2]
		entry, ok := f.dirMap.Hash[dirHash]
		if !ok {
			fs.LogPrintf(fs.LogLevelWarning, nil, "cannot map change notification for path %q", path)
			return
		}
		files, err := entry.Files(ctx)
		if err != nil {
			fs.LogPrintf(fs.LogLevelError, nil, "cannot fetch map file for path %q: %w", path, err)
			return
		}
		for path, hash := range files {
			if hash == fileHash {
				notify(path, typ)
				return
			}
		}
		fs.LogPrintf(fs.LogLevelWarning, nil, "no file matches while mapping change notification for path %q", path)
	}
	do(ctx, wrappedNotify, interval)
}

// DirMove moves the specified directory from srcRemote to dstRemote after
// mapping both remotes.
func (f *Fs) DirMove(ctx context.Context, src fs.Fs, srcRemote, dstRemote string) error {
	do := f.base.Features().DirMove
	if do == nil {
		return fs.ErrorCantDirMove
	}
	srcFs := src.(*Fs)
	srcRemote = path.Join(srcFs.root, srcRemote)
	dstRemote = path.Join(f.root, dstRemote)
	srcEntry, ok := srcFs.dirMap.Path[srcRemote]
	if !ok {
		return fs.ErrorDirNotFound
	}
	if _, ok := f.dirMap.Path[dstRemote]; ok {
		return fs.ErrorDirExists
	}
	var recurse func(entry *dirEntry) error
	recurse = func(entry *dirEntry) error {
		// Process children first to be sure parent directories always exist.
		for _, child := range entry.Children {
			if err := recurse(child); err != nil {
				return err
			}
		}
		// Move the directory from the source to the target.
		srcRelative := strings.TrimPrefix(entry.Path, srcRemote)
		srcRelative = strings.TrimPrefix(srcRelative, "/")
		dstLocation := path.Join(dstRemote, srcRelative)
		srcHash := srcFs.hasher(entry.Path)
		dstHash := f.hasher(dstLocation)
		if err := do(ctx, srcFs.base, srcHash, dstHash); err != nil {
			return err
		}
		// Modify the directory maps.
		f.dirMap.newDirEntry(dstLocation)
		srcFs.dirMap.removeEntry(entry.Path)
		// Rewrite the name files.
		return f.rewriteNameFiles(ctx, dstLocation)
	}
	recurseErr := recurse(srcEntry)
	if err := f.dirMap.write(ctx); err != nil {
		return err
	}
	if err := srcFs.dirMap.write(ctx); err != nil {
		return err
	}
	return recurseErr
}

// rewriteNameFiles rewrites the name files in the specified directory.
// dstLocation is the absolute location. It does not write name files
// recursively.
func (f *Fs) rewriteNameFiles(ctx context.Context, dstLocation string) error {
	entry := f.dirMap.Path[dstLocation]
	// Fetch list of files to rewrite name files.
	files, err := entry.Files(ctx)
	if err != nil {
		return fmt.Errorf("cannot rewrite name files with invalid map file: %w", err)
	}
	// Rewrite name files to fit new path.
	for fileName, hash := range files {
		obj, err := f.base.NewObject(ctx, path.Join(entry.Hash, hash, "name"))
		if err != nil {
			return fmt.Errorf("cannot find name file to delete: %w", err)
		}
		if err := obj.Remove(ctx); err != nil {
			return fmt.Errorf("cannot delete name file: %w", err)
		}
		fileDst := path.Join(dstLocation, fileName)
		objInfo := fakeObjInfo{
			remote: path.Join(entry.Hash, hash, "name"),
			fs:     f,
			size:   int64(len(fileDst) + 1),
		}
		if _, err := f.base.Put(ctx, strings.NewReader(fileDst+"\n"), objInfo); err != nil {
			return err
		}
	}
	return nil
}

// Purge purges all files in the directory specified by recursively going into
// directories and invoking Purge on all subdirectories.
func (f *Fs) Purge(ctx context.Context, dir string) error {
	do := f.base.Features().Purge
	if do == nil {
		return fs.ErrorCantPurge
	}
	dir = path.Join(f.root, dir)
	entry, ok := f.dirMap.Path[dir]
	if !ok {
		return fs.ErrorDirNotFound
	}
	var purge func(*dirEntry) error
	purge = func(entry *dirEntry) error {
		// Purge subdirectories.
		for _, v := range entry.Children {
			if err := purge(v); err != nil {
				return err
			}
		}
		// Remove the directory from the backing Fs.
		if err := do(ctx, entry.Hash); err != nil {
			return err
		}
		// Remove from internal buffer.
		delete(f.dirMap.Path, entry.Path)
		delete(f.dirMap.Hash, entry.Hash)
		return nil
	}
	purgeErr := purge(entry)
	err := f.dirMap.write(ctx)
	if err != nil {
		return err
	}
	return purgeErr
}

// directory is an implementation of DirEntry that represents a directory.
type directory struct {
	// dir is the underlying base directory if available, it is nil otherwise.
	dir fs.Directory
	// entry is the directory entry that the directory represents.
	entry *dirEntry
}

var _ fs.Directory = directory{}

// String returns the string representation of the directory.
func (d directory) String() string {
	return d.entry.Path
}

// Remote returns the actual path of the directory.
func (d directory) Remote() string {
	return d.entry.Path
}

// ModTime returns the modification time as reported by the base directory.
func (d directory) ModTime(ctx context.Context) time.Time {
	if d.dir == nil {
		return time.Now()
	}
	return d.dir.ModTime(ctx)
}

// Size returns the size as reported by the base directory.
func (d directory) Size() int64 {
	if d.dir == nil {
		return -1
	}
	return d.dir.Size()
}

// Items returns the number of files in the directory.
func (d directory) Items() int64 {
	// Note: d.entry.files is directly accessed here to use the cached version
	// if available. Fetching file list is expensive for this operation.
	return int64(len(d.entry.Children) + len(d.entry.files))
}

// ID returns an empty string to represent that the internal ID of the
// directory is not known.
func (d directory) ID() string {
	return ""
}
